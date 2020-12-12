from aws_cdk import (
    aws_iam as iam,
    aws_sns as sns,
    aws_sns_subscriptions as sns_subs,
    aws_sqs as sqs,
    aws_s3 as s3,
    aws_dynamodb as dynamodb,
    aws_lambda as lambda_,
    aws_events as events,
    core
)
from aws_cdk.aws_lambda_event_sources import S3EventSource, DynamoEventSource, SqsEventSource, SnsEventSource
from aws_cdk.aws_events_targets import LambdaFunction


class TextractPipelineStack(core.Stack):

    def __init__(self, scope: core.Construct, construct_id: str, **kwargs) -> None:
        super().__init__(scope, construct_id, **kwargs)

        # SNS Topics
        job_completion_topic = sns.Topic(self, "JobCompletion")

        # IAM Roles
        textract_service_role = iam.Role(
            self, "TextractServiceRole", assumed_by=iam.ServicePrincipal('textract.amazonaws.com')
        )
        textract_service_role.add_to_policy(
            iam.PolicyStatement(
                effect=iam.Effect.ALLOW,
                resources=[job_completion_topic.topic_arn],
                actions=["sns:Publish"]
            )
        )

        # Role : S3 Batch Operation
        s3_batch_operations_role = iam.Role(self, 'S3BatchOperationsRole',
                                            assumed_by=iam.ServicePrincipal('batchoperations.s3.amazonaws.com')
                                            )

        # S3 Buckets
        # S3 Bucket for input and output documents
        content_bucket = s3.Bucket(self, 'DocumentsBucket', versioned=False)
        existing_content_bucket = s3.Bucket(self, 'ExistingDocumentsBucket', versioned=False)
        existing_content_bucket.grant_read_write(s3_batch_operations_role)

        inventory_and_logs_bucket = s3.Bucket(self, 'InventoryAndLogsBucket', versioned=False)
        inventory_and_logs_bucket.grant_read_write(s3_batch_operations_role)

        # DynamoDB Tables
        document_table = dynamodb.Table(self, 'DocumentsTable',
                                        partition_key=dynamodb.Attribute(
                                            name='documentId', type=dynamodb.AttributeType.STRING
                                        ),
                                        stream=dynamodb.StreamViewType.NEW_IMAGE
                                        )

        output_table = dynamodb.Table(self, 'OutputTable',
                                      partition_key=dynamodb.Attribute(
                                          name='documentId', type=dynamodb.AttributeType.STRING
                                      ),
                                      sort_key=dynamodb.Attribute(
                                          name='outputType', type=dynamodb.AttributeType.STRING
                                      )
                                      )

        # SQS Queues
        dlq = sqs.Queue(self, 'DLQ',
                        visibility_timeout=core.Duration.seconds(30),
                        retention_period=core.Duration.seconds(1209600)
                        )
        sync_jobs_queue = sqs.Queue(self, 'SyncJobs',
                                    visibility_timeout=core.Duration.seconds(30),
                                    retention_period=core.Duration.seconds(1209600),
                                    dead_letter_queue=sqs.DeadLetterQueue(queue=dlq, max_receive_count=50)
                                    )
        async_jobs_queue = sqs.Queue(self, 'AsyncJobs',
                                     visibility_timeout=core.Duration.seconds(30),
                                     retention_period=core.Duration.seconds(1209600),
                                     dead_letter_queue=sqs.DeadLetterQueue(queue=dlq, max_receive_count=50)
                                     )

        jobs_result_queue = sqs.Queue(self, 'JobsResult',
                                      visibility_timeout=core.Duration.seconds(900),
                                      retention_period=core.Duration.seconds(1209600),
                                      dead_letter_queue=sqs.DeadLetterQueue(queue=dlq, max_receive_count=50)
                                      )

        job_completion_topic.add_subscription(sns_subs.SqsSubscription(jobs_result_queue))

        # Lambda Functions

        # Helper Layers
        helper_layer = lambda_.LayerVersion(self, 'HelperLayer',
                                            code=lambda_.Code.from_asset('layers/helper'),
                                            compatible_runtimes=[lambda_.Runtime.PYTHON_3_8],
                                            license='Apache-2.0',
                                            description='Helper Layer'
                                            )

        textractor_layer = lambda_.LayerVersion(self, 'TextractLayer',
                                                code=lambda_.Code.from_asset('layers/textractor'),
                                                compatible_runtimes=[lambda_.Runtime.PYTHON_3_8],
                                                license='Apache-2.0',
                                                description='Textract Layer'
                                                )

        # S3 Event Processor
        s3_processor = lambda_.Function(self, 'S3Processor',
                                        runtime=lambda_.Runtime.PYTHON_3_8,
                                        code=lambda_.Code.from_asset('lambda/s3_processor'),
                                        handler='main.lambda_handler',
                                        timeout=core.Duration.seconds(30),
                                        environment=dict(SYNC_QUEUE_URL=sync_jobs_queue.queue_url,
                                                         ASYNC_QUEUE_URL=async_jobs_queue.queue_url,
                                                         DOCUMENTS_TABLE=document_table.table_name,
                                                         OUTPUT_TABLE=output_table.table_name)
                                        )

        s3_processor.add_layers(helper_layer)
        s3_processor.add_event_source(S3EventSource(bucket=content_bucket,
                                                    events=[s3.EventType.OBJECT_CREATED],
                                                    filters=[s3.NotificationKeyFilter(suffix=".pdf")]
                                                    )
                                      )
        s3_processor.add_event_source(S3EventSource(bucket=content_bucket,
                                                    events=[s3.EventType.OBJECT_CREATED],
                                                    filters=[s3.NotificationKeyFilter(suffix=".jpeg")]
                                                    )
                                      )
        s3_processor.add_event_source(S3EventSource(bucket=content_bucket,
                                                    events=[s3.EventType.OBJECT_CREATED],
                                                    filters=[s3.NotificationKeyFilter(suffix=".jpg")]
                                                    )
                                      )
        s3_processor.add_event_source(S3EventSource(bucket=content_bucket,
                                                    events=[s3.EventType.OBJECT_CREATED],
                                                    filters=[s3.NotificationKeyFilter(suffix=".png")]
                                                    )
                                      )

        # Permissions
        document_table.grant_read_write_data(s3_processor)
        sync_jobs_queue.grant_send_messages(s3_processor)
        async_jobs_queue.grant_send_messages(s3_processor)

        # S3 Batch Operations Events Processor
        s3_batch_processor = lambda_.Function(self, 'S3BatchProcessor',
                                              runtime=lambda_.Runtime.PYTHON_3_8,
                                              code=lambda_.Code.from_asset('lambda/s3_batch_processor'),
                                              handler='main.lambda_handler',
                                              timeout=core.Duration.seconds(30),
                                              reserved_concurrent_executions=1,
                                              environment=dict(DOCUMENTS_TABLE=document_table.table_name,
                                                               OUTPUT_TABLE=output_table.table_name)
                                              )

        s3_batch_processor.add_layers(helper_layer)
        document_table.grant_read_write_data(s3_batch_processor)
        s3_batch_processor.grant_invoke(s3_batch_operations_role)
        s3_batch_operations_role.add_to_policy(iam.PolicyStatement(actions=['lambda:*'], resources=['*']))

        # Document Processor (Router to sync/async pipeline)
        document_processor = lambda_.Function(self, 'TaskProcessor',
                                              runtime=lambda_.Runtime.PYTHON_3_8,
                                              code=lambda_.Code.from_asset('lambda/document_processor'),
                                              handler='main.lambda_handler',
                                              timeout=core.Duration.seconds(900),
                                              environment=dict(SYNC_QUEUE_URL=sync_jobs_queue.queue_url,
                                                               ASYNC_QUEUE_URL=async_jobs_queue.queue_url)
                                              )

        document_processor.add_layers(helper_layer)
        document_processor.add_event_source(DynamoEventSource(document_table,
                                                              starting_position=lambda_.StartingPosition.TRIM_HORIZON)
                                            )
        # Permissions
        document_table.grant_read_write_data(document_processor)
        sync_jobs_queue.grant_send_messages(document_processor)
        async_jobs_queue.grant_send_messages(document_processor)

        # Sync Job Processor (Process jobs using sync APIs)
        sync_processor = lambda_.Function(self, 'SyncProcessor',
                                          runtime=lambda_.Runtime.PYTHON_3_8,
                                          code=lambda_.Code.from_asset('lambda/sync_processor'),
                                          handler='main.lambda_handler',
                                          timeout=core.Duration.seconds(25),
                                          reserved_concurrent_executions=1,
                                          environment=dict(OUTPUT_TABLE=output_table.table_name,
                                                           DOCUMENTS_TABLE=document_table.table_name,
                                                           AWS_DATA_PATH='models')
                                          )

        sync_processor.add_layers(*[helper_layer, textractor_layer])
        sync_processor.add_event_source(SqsEventSource(sync_jobs_queue, batch_size=1))

        # Permissions
        content_bucket.grant_read_write(sync_processor)
        existing_content_bucket.grant_read_write(sync_processor)
        output_table.grant_read_write_data(sync_processor)
        document_table.grant_read_write_data(sync_processor)
        sync_processor.add_to_role_policy(iam.PolicyStatement(actions=['textract:*'], resources=['*']))

        # Async Job Processor (Start jobs using Async APIs)
        async_processor = lambda_.Function(self, 'AsyncProcessor',
                                           runtime=lambda_.Runtime.PYTHON_3_8,
                                           code=lambda_.Code.from_asset('lambda/async_processor'),
                                           handler='main.lambda_handler',
                                           timeout=core.Duration.seconds(60),
                                           reserved_concurrent_executions=1,
                                           environment=dict(ASYNC_QUEUE_URL=async_jobs_queue.queue_url,
                                                            SNS_TOPIC_ARN=job_completion_topic.topic_arn,
                                                            SNS_ROLE_ARN=textract_service_role.role_arn,
                                                            AWS_DATA_PATH='models'
                                                            )
                                           )
        async_processor.add_layers(helper_layer)

        # Triggers
        rule = events.Rule(self, 'Rule', schedule=events.Schedule.expression(expression='rate(2 minutes)'))
        rule.add_target(LambdaFunction(async_processor))
        async_processor.add_event_source(SnsEventSource(job_completion_topic))
        content_bucket.grant_read(async_processor)
        existing_content_bucket.grant_read_write(async_processor)
        async_jobs_queue.grant_consume_messages(async_processor)
        async_processor.add_to_role_policy(iam.PolicyStatement(actions=['iam:PassRole'], resources=['*']))

        # Async Jobs Results Processor
        job_result_processor = lambda_.Function(self, 'JobResultProcessor',
                                                runtime=lambda_.Runtime.PYTHON_3_8,
                                                code=lambda_.Code.from_asset('lambda/job_result_processor'),
                                                handler='main.lambda_handler',
                                                memory_size=2000,
                                                timeout=core.Duration.seconds(900),
                                                reserved_concurrent_executions=2,  # 50
                                                environment=dict(OUTPUT_TABLE=output_table.table_name,
                                                                 DOCUMENTS_TABLE=document_table.table_name,
                                                                 AWS_DATA_PATH='models'
                                                                 )
                                                )

        job_result_processor.add_layers(*[helper_layer, textractor_layer])

        job_result_processor.add_event_source(SqsEventSource(jobs_result_queue, batch_size=1))

        # Permissions
        output_table.grant_read_write_data(job_result_processor)
        document_table.grant_read_write_data(job_result_processor)
        content_bucket.grant_read_write(job_result_processor)
        existing_content_bucket.grant_read_write(job_result_processor)
        job_result_processor.add_to_role_policy(iam.PolicyStatement(actions=['textract:*'], resources=['*']))

        # PDF Generator
        pdf_generator = lambda_.Function(self, 'PdfGenerator',
                                         runtime=lambda_.Runtime.JAVA_8,
                                         code=lambda_.Code.from_asset('lambda/pdf_generator'),
                                         handler='DemoLambdaV2.handlerRequest',
                                         memory_size=3000,
                                         timeout=core.Duration.seconds(900)
                                         )
        content_bucket.grant_read_write(pdf_generator)
        existing_content_bucket.grant_read_write(pdf_generator)
        pdf_generator.grant_invoke(sync_processor)
        pdf_generator.grant_invoke(async_processor)
