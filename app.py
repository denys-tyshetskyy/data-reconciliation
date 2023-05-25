#!/usr/bin/env python3
import os

import aws_cdk as cdk

from data_reconciliation.data_reconsiliation_stack import ReconciliationStack


app = cdk.App()
ReconciliationStack(app, "DataReconciliationStack", env=cdk.Environment(
                    account="123",
                    region="ap-southeast-2",
                ))

app.synth()
