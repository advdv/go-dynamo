package main

import (
	"fmt"
	"os"
	"path/filepath"
	"reflect"
	"runtime"
	"testing"

	"github.com/aws/aws-sdk-go/aws"
	"github.com/aws/aws-sdk-go/aws/credentials"
	"github.com/aws/aws-sdk-go/aws/session"
)

// tablename returns the test table name or fail the test
func tablename(tb testing.TB) string {
	tname := os.Getenv("TEST_TABLE_NAME")
	if tname == "" {
		tb.Fatalf("env variable TEST_TABLE_NAME is empty, make sure it is set to the dynamodb table name you would like to use for integration testing")
	}

	return tname
}

// newsess will try to setup an aws session from the environment or fail
func newsess(tb testing.TB) *session.Session {
	cfg := &aws.Config{
		Credentials: credentials.NewEnvCredentials(),
	}

	sess, err := session.NewSession(cfg)
	if err != nil {
		tb.Fatal("failed to setup aws session", err)
	}

	return sess
}

// assert fails the test if the condition is false.
func assert(tb testing.TB, condition bool, msg string, v ...interface{}) {
	if !condition {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: "+msg+"\033[39m\n\n", append([]interface{}{filepath.Base(file), line}, v...)...)
		tb.FailNow()
	}
}

// ok fails the test if an err is not nil.
func ok(tb testing.TB, err error) {
	if err != nil {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d: unexpected error: %s\033[39m\n\n", filepath.Base(file), line, err.Error())
		tb.FailNow()
	}
}

// equals fails the test if exp is not equal to act.
func equals(tb testing.TB, exp, act interface{}) {
	if !reflect.DeepEqual(exp, act) {
		_, file, line, _ := runtime.Caller(1)
		fmt.Printf("\033[31m%s:%d:\n\n\texp: %#v\n\n\tgot: %#v\033[39m\n\n", filepath.Base(file), line, exp, act)
		tb.FailNow()
	}
}
