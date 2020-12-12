import json
import pytest

from aws_cdk import core
from textract-pipeline.textract_pipeline_stack import TextractPipelineStack


def get_template():
    app = core.App()
    TextractPipelineStack(app, "textract-pipeline")
    return json.dumps(app.synth().get_stack("textract-pipeline").template)


def test_sqs_queue_created():
    assert("AWS::SQS::Queue" in get_template())


def test_sns_topic_created():
    assert("AWS::SNS::Topic" in get_template())
