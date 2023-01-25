import aws_cdk as core
import aws_cdk.assertions as assertions

from data_masking.data_masking_stack import DataMaskingStack

# example tests. To run these tests, uncomment this file along with the example
# resource in data_masking/data_masking_stack.py
def test_sqs_queue_created():
    app = core.App()
    stack = DataMaskingStack(app, "data-masking")
    template = assertions.Template.from_stack(stack)

#     template.has_resource_properties("AWS::SQS::Queue", {
#         "VisibilityTimeout": 300
#     })
