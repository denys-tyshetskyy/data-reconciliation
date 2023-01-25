#!/usr/bin/env python3
import os

import aws_cdk as cdk

from data_masking.data_masking_stack import DataMaskingStack


app = cdk.App()
DataMaskingStack(app, "DataMaskingStack")

app.synth()
