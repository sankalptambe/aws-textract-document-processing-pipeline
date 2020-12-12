#!/usr/bin/env python3

from aws_cdk import core

from textract_pipeline.textract_pipeline_stack import TextractPipelineStack


app = core.App()
TextractPipelineStack(app, "textract-pipeline", env={'region': 'ap-south-1'})

app.synth()
