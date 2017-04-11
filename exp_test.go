package dynamo

import (
	"testing"

	"github.com/aws/aws-sdk-go/aws"
)

func TestExp(t *testing.T) {
	exp, names, vals, err := Exp("attribute_not_exists(#wrk)").
		N("#wrk", "wrk").
		V(":foo", "bar").
		Get()

	ok(t, err)
	equals(t, "attribute_not_exists(#wrk)", aws.StringValue(exp))
	assert(t, len(names) == 1, "should have one name, got: %#v", names)
	assert(t, len(vals) == 1, "should have one value, got: %#v", vals)

	equals(t, "wrk", aws.StringValue(names["#wrk"]))
	equals(t, "bar", aws.StringValue(vals[":foo"].S))
}
