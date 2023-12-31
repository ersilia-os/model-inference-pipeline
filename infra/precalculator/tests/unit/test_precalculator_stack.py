import aws_cdk as core
import aws_cdk.assertions as assertions

from precalculator.precalculator_stack import PrecalculatorStack


# example tests. To run these tests, uncomment this file along with the example
# resource in precalculator/precalculator_stack.py
def test_table_created():
    app = core.App()
    stack = PrecalculatorStack(app, "precalculator")
    template = assertions.Template.from_stack(stack)

    template.has_resource_properties("AWS::DynamoDB::GlobalTable", {"BillingMode": "PAY_PER_REQUEST"})
