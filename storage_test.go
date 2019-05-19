package storage

import (
	"encoding/json"
	"fmt"
	"io/ioutil"
	"testing"
	"time"

	lbcf "github.com/lidstromberg/config"

	context "golang.org/x/net/context"
)

var (
	testBucket = "{{testbucket}}"
	testFile   = "storagetester.json"
	testPrefix = "bucketprefix"
)

//GetLocalFileData returns a byte array for a local file
func getLocalFileData(fileName string) ([]byte, error) {
	fileBytes, err := ioutil.ReadFile(fileName)
	if err != nil {
		return nil, err
	}

	return fileBytes, nil
}
func Test_NewMgr(t *testing.T) {
	ctx := context.Background()

	//create a new config object
	bc := lbcf.NewConfig(ctx)

	//create a new storage object
	_, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}
}
func Test_WriteBucketFile(t *testing.T) {
	ctx := context.Background()

	//create a new config object
	bc := lbcf.NewConfig(ctx)

	//create a new storage object
	sto, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	//read in the file data
	dat, err := getLocalFileData(testFile)
	if err != nil {
		t.Fatal(err)
	}

	//write to the test bucket
	err = sto.WriteBucketFile(ctx, testBucket, "json", testFile, dat)
	if err != nil {
		t.Fatal(err)
	}
}
func Test_GetBucketFileData(t *testing.T) {
	ctx := context.Background()

	//create a new config object
	bc := lbcf.NewConfig(ctx)

	//create a new storage object
	sto, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	//read the data back from the bucket
	dat, err := sto.GetBucketFileData(ctx, testBucket, testFile)
	if err != nil {
		t.Fatal(err)
	}

	//unmarshal the data into a struct
	type storagetest struct {
		Title string `json:"testtitle"`
	}

	var st storagetest
	err = json.Unmarshal(dat, &st)
	if err != nil {
		t.Fatal(err)
	}

	//check that the message is the same as the one which was written in the earlier test
	if st.Title != "this is a successful storage test" {
		t.Fatal("test file content message is not correct")
	}
}

func Test_RemoveFile(t *testing.T) {
	ctx := context.Background()

	//create a new config object
	bc := lbcf.NewConfig(ctx)

	//create a new storage object
	sto, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	//remove the file from the bucket
	err = sto.RemoveFile(ctx, testBucket, "json", testFile)
	if err != nil {
		t.Fatal(err)
	}
}

func Test_ListBucketNoPrefix(t *testing.T) {
	ctx := context.Background()

	//create a new config object
	bc := lbcf.NewConfig(ctx)

	//create a new storage object
	sto, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	//list the bucket (obtain the channel)
	rfchn, err := sto.ListBucket(ctx, testBucket, "", 100)
	//if an error is returned here, then the channel is nil
	if err != nil {
		t.Fatal(err)
	}

	//defer the channel drain (in case there is an error before the channel producer finishes writing)
	defer DrainFn(rfchn)

	//create an object attribute subset
	at := make(map[string]interface{})

	//assume the channel state is open
	ischopen := true
	for ischopen {
		select {
		//if the context Done has been received, then reset ischopen and break out of the loop..
		case <-ctx.Done():
			{
				ischopen = false
				break
			}
			//or.. read from the channel
		case r, ok := <-rfchn:
			//..if is not ok, then the channel has been closed by the producer, so reset ischopen and break..
			if ok == false {
				ischopen = false
				break
			}
			//..if is ok and is castable to error, then it's an error.. reset ischopen and test log as fatal
			if _, ok := r.(error); ok {
				err := r.(error)
				ischopen = false
				t.Fatal(err)
			}
			//..if is ok and is castable to ObjectAttrSubset then it's a ObjectAttrSubset
			if _, ok := r.(map[string]interface{}); ok {
				at = r.(map[string]interface{})

				//convert the int64 back to a time
				tm := time.Unix(at[ObjAttrCreated].(int64), 0)
				fmt.Println(tm)

				fmt.Printf("%s\t%v\n", at[ObjAttrName], tm)
			}
		}
	}
}

func Test_ListBucketWithPrefix(t *testing.T) {
	ctx := context.Background()

	//create a new config object
	bc := lbcf.NewConfig(ctx)

	//create a new storage object
	sto, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	//list the bucket (obtain the channel)
	rfchn, err := sto.ListBucket(ctx, testBucket, testPrefix, 100)
	//if an error is returned here, then the channel is nil
	if err != nil {
		t.Fatal(err)
	}

	//defer the channel drain (in case there is an error before the channel producer finishes writing)
	defer DrainFn(rfchn)

	//create an object attribute subset
	at := make(map[string]interface{})

	//assume the channel state is open
	ischopen := true
	for ischopen {
		select {
		//if the context Done has been received, then reset ischopen and break out of the loop..
		case <-ctx.Done():
			{
				ischopen = false
				break
			}
			//or.. read from the channel
		case r, ok := <-rfchn:
			//..if is not ok, then the channel has been closed by the producer, so reset ischopen and break..
			if ok == false {
				ischopen = false
				break
			}
			//..if is ok and is castable to error, then it's an error.. reset ischopen and test log as fatal
			if _, ok := r.(error); ok {
				err := r.(error)
				ischopen = false
				t.Fatal(err)
			}
			//..if is ok and is castable to ObjectAttrSubset then it's a ObjectAttrSubset
			if _, ok := r.(map[string]interface{}); ok {
				at = r.(map[string]interface{})

				//convert the int64 back to a time
				tm := time.Unix(at[ObjAttrCreated].(int64), 0)
				fmt.Println(tm)

				fmt.Printf("%s\t%v\n", at[ObjAttrName], tm)
			}
		}
	}
}

func Test_ListBucketByTimeNoPrefixOpenDate(t *testing.T) {
	ctx := context.Background()

	//create a new config object
	bc := lbcf.NewConfig(ctx)

	//create a new storage object
	sto, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now().AddDate(-1, 0, 0)
	end := time.Now()

	//list the bucket (obtain the channel)
	rfchn, err := sto.ListBucketByTime(ctx, testBucket, "", &start, &end, 100)
	//if an error is returned here, then the channel is nil
	if err != nil {
		t.Fatal(err)
	}

	//defer the channel drain (in case there is an error before the channel producer finishes writing)
	defer DrainFn(rfchn)

	//create an object attribute subset
	at := make(map[string]interface{})

	//assume the channel state is open
	ischopen := true
	for ischopen {
		select {
		//if the context Done has been received, then reset ischopen and break out of the loop..
		case <-ctx.Done():
			{
				ischopen = false
				break
			}
			//or.. read from the channel
		case r, ok := <-rfchn:
			//..if is not ok, then the channel has been closed by the producer, so reset ischopen and break..
			if ok == false {
				ischopen = false
				break
			}
			//..if is ok and is castable to error, then it's an error.. reset ischopen and test log as fatal
			if _, ok := r.(error); ok {
				err := r.(error)
				ischopen = false
				t.Fatal(err)
			}
			//..if is ok and is castable to ObjectAttrSubset then it's a ObjectAttrSubset
			if _, ok := r.(map[string]interface{}); ok {
				at = r.(map[string]interface{})

				//convert the int64 back to a time
				tm := time.Unix(at[ObjAttrCreated].(int64), 0)
				fmt.Println(tm)

				fmt.Printf("%s\t%v\n", at[ObjAttrName], tm)
			}
		}
	}
}
func Test_ListBucketByTimeNoPrefixClosedDate(t *testing.T) {
	ctx := context.Background()

	//create a new config object
	bc := lbcf.NewConfig(ctx)

	//create a new storage object
	sto, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now().AddDate(0, 0, -20)
	end := time.Now()

	//list the bucket (obtain the channel)
	rfchn, err := sto.ListBucketByTime(ctx, testBucket, "", &start, &end, 100)
	//if an error is returned here, then the channel is nil
	if err != nil {
		t.Fatal(err)
	}

	//defer the channel drain (in case there is an error before the channel producer finishes writing)
	defer DrainFn(rfchn)

	//create an object attribute subset
	at := make(map[string]interface{})

	//assume the channel state is open
	ischopen := true
	for ischopen {
		select {
		//if the context Done has been received, then reset ischopen and break out of the loop..
		case <-ctx.Done():
			{
				ischopen = false
				break
			}
			//or.. read from the channel
		case r, ok := <-rfchn:
			//..if is not ok, then the channel has been closed by the producer, so reset ischopen and break..
			if ok == false {
				ischopen = false
				break
			}
			//..if is ok and is castable to error, then it's an error.. reset ischopen and test log as fatal
			if _, ok := r.(error); ok {
				err := r.(error)
				ischopen = false
				t.Fatal(err)
			}
			//..if is ok and is castable to ObjectAttrSubset then it's a ObjectAttrSubset
			if _, ok := r.(map[string]interface{}); ok {
				at = r.(map[string]interface{})

				//convert the int64 back to a time
				tm := time.Unix(at[ObjAttrCreated].(int64), 0)
				fmt.Println(tm)

				fmt.Printf("%s\t%v\n", at[ObjAttrName], tm)
			}
		}
	}
}
func Test_ListBucketByTimeWithPrefixClosedDate(t *testing.T) {
	ctx := context.Background()

	//create a new config object
	bc := lbcf.NewConfig(ctx)

	//create a new storage object
	sto, err := NewMgr(ctx, bc)
	if err != nil {
		t.Fatal(err)
	}

	start := time.Now().AddDate(0, 0, -20)
	end := time.Now()

	//list the bucket (obtain the channel)
	rfchn, err := sto.ListBucketByTime(ctx, testBucket, testPrefix, &start, &end, 100)
	//if an error is returned here, then the channel is nil
	if err != nil {
		t.Fatal(err)
	}

	//defer the channel drain (in case there is an error before the channel producer finishes writing)
	defer DrainFn(rfchn)

	//create an object attribute subset
	at := make(map[string]interface{})

	//assume the channel state is open
	ischopen := true
	for ischopen {
		select {
		//if the context Done has been received, then reset ischopen and break out of the loop..
		case <-ctx.Done():
			{
				ischopen = false
				break
			}
			//or.. read from the channel
		case r, ok := <-rfchn:
			//..if is not ok, then the channel has been closed by the producer, so reset ischopen and break..
			if ok == false {
				ischopen = false
				break
			}
			//..if is ok and is castable to error, then it's an error.. reset ischopen and test log as fatal
			if _, ok := r.(error); ok {
				err := r.(error)
				ischopen = false
				t.Fatal(err)
			}
			//..if is ok and is castable to ObjectAttrSubset then it's a ObjectAttrSubset
			if _, ok := r.(map[string]interface{}); ok {
				at = r.(map[string]interface{})

				//convert the int64 back to a time
				tm := time.Unix(at[ObjAttrCreated].(int64), 0)
				fmt.Println(tm)

				fmt.Printf("%s\t%v\n", at[ObjAttrName], tm)
			}
		}
	}
}
