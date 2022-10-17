package storage

import (
	"io/ioutil"
	"sync"
	"time"

	lbcf "github.com/lidstromberg/config"
	lblog "github.com/lidstromberg/log"

	"cloud.google.com/go/storage"
	"golang.org/x/net/context"
	"google.golang.org/api/iterator"
	"google.golang.org/api/option"
)

//StorMgr handles interactions with GCS
type StorMgr struct {
	st *storage.Client
	bc lbcf.ConfigSetting
}

//NewMgr returns a new storage manager
func NewMgr(ctx context.Context, bc lbcf.ConfigSetting) (*StorMgr, error) {
	preflight(ctx, bc)

	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "NewMgr", "info", "start")
	}

	storageClient, err := storage.NewClient(ctx, option.WithGRPCConnectionPool(EnvClientPool))
	if err != nil {
		return nil, err
	}

	st1 := &StorMgr{
		st: storageClient,
		bc: bc,
	}

	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "NewMgr", "info", "end")
	}

	return st1, nil
}

//NewJSONMgr returns a new storage manager based on a GCP credential supplied as a byte array
func NewJSONMgr(ctx context.Context, bc lbcf.ConfigSetting, cred []byte) (*StorMgr, error) {
	preflight(ctx, bc)

	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "NewJSONMgr", "info", "start")
	}

	storageClient, err := storage.NewClient(ctx, option.WithGRPCConnectionPool(EnvClientPool), option.WithCredentialsJSON(cred))
	if err != nil {
		return nil, err
	}

	st1 := &StorMgr{
		st: storageClient,
		bc: bc,
	}

	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "NewJSONMgr", "info", "end")
	}

	return st1, nil
}

//GetBucketFileData returns a byte array for a bucket file
func (sto *StorMgr) GetBucketFileData(ctx context.Context, bucketName string, fileName string) ([]byte, error) {
	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "GetBucketFileData", "info", "start")
	}

	rc, err := sto.st.Bucket(bucketName).Object(fileName).NewReader(ctx)
	defer func(rc *storage.Reader) {
		err := rc.Close()
		if err != nil {
		}
	}(rc)

	if err != nil {
		return nil, err
	}

	data, err := ioutil.ReadAll(rc)
	if err != nil {
		return nil, err
	}

	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "GetBucketFileData", "info", "end")
	}

	return data, nil
}

//WriteBucketFile writes a file byte array to a bucket file
func (sto *StorMgr) WriteBucketFile(ctx context.Context, bucketName string, fileName string, data []byte) error {
	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "WriteBucketFile", "info", "start")
	}

	wc := sto.st.Bucket(bucketName).Object(fileName).NewWriter(ctx)
	defer func(wc *storage.Writer) {
		err := wc.Close()
		if err != nil {
		}
	}(wc)

	if _, err := wc.Write(data); err != nil {
		return err
	}

	if err := wc.Close(); err != nil {
		return err
	}

	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "WriteBucketFile", "info", "end")
	}

	return nil
}

//ListBucket returns a configurable buffered channel which contains a subset of object metadata
func (sto *StorMgr) ListBucket(ctx context.Context, bucketName, prefix string, bufferSize int) (<-chan interface{}, error) {

	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "ListBucket", "info", "start")
	}

	//waitgroup to control goroutines
	var wg sync.WaitGroup

	//create the channel
	result := make(chan interface{}, bufferSize)

	//bucket listing function
	rfn := func(bucketname, prefix string) {
		lbucketName := bucketname
		lprefix := prefix

		defer wg.Done()

		//get an iterator to the target bucket
		it := &storage.ObjectIterator{}

		//if a query prefix was supplied, then use it
		if lprefix != "" {
			qr := &storage.Query{
				Prefix: lprefix,
			}

			itx := sto.st.Bucket(lbucketName).Objects(ctx, qr)
			it = itx
		} else {
			itx := sto.st.Bucket(lbucketName).Objects(ctx, nil)
			it = itx
		}

		//collect the subset of attributes
		for {
			attrs, err := it.Next()
			if err != nil {
				if err == iterator.Done {
					return
				}

				//send back the error if any occur
				result <- err
				return
			}

			//collect the attributes
			at := make(map[string]interface{})

			at[ObjAttrName] = attrs.Name
			at[ObjAttrContentType] = attrs.ContentType
			at[ObjAttrOwner] = attrs.Owner
			at[ObjAttrSize] = attrs.Size
			at[ObjAttrContentEncoding] = attrs.ContentEncoding
			at[ObjAttrCreated] = attrs.Created.Unix()

			select {
			case <-ctx.Done():
				return
			default:
				result <- at
			}
		}
	}

	wg.Add(1)
	go rfn(bucketName, prefix)

	go func() {
		wg.Wait()
		close(result)
	}()

	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "ListBucket", "info", "end")
	}

	return result, nil
}

//ListBucketByTime returns a configurable buffered channel which contains a subset of object metadata
func (sto *StorMgr) ListBucketByTime(ctx context.Context, bucketName, prefix string, start, end *time.Time, bufferSize int) (<-chan interface{}, error) {

	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "ListBucketByTime", "info", "start")
	}

	//if the dates are not available, then exit with error
	if start == nil || end == nil {
		return nil, ErrMissingDateRange
	}

	//waitgroup to control goroutines
	var wg sync.WaitGroup

	//create the channel
	result := make(chan interface{}, bufferSize)

	//bucket listing function
	rfn := func(bucketname, prefix string) {
		lbucketName := bucketname
		lprefix := prefix

		defer wg.Done()

		//get an iterator to the target bucket
		it := &storage.ObjectIterator{}

		//if a query prefix was supplied, then use it
		if lprefix != "" {
			qr := &storage.Query{
				Prefix: lprefix,
			}

			itx := sto.st.Bucket(lbucketName).Objects(ctx, qr)
			it = itx
		} else {
			itx := sto.st.Bucket(lbucketName).Objects(ctx, nil)
			it = itx
		}

		//collect the subset of attributes
		for {
			attrs, err := it.Next()
			if err != nil {
				if err == iterator.Done {
					return
				}

				//send back the error if any occur
				result <- err
				return
			}

			//collect the object attributes if the object is created within the required date range
			if attrs.Created.After(*start) && attrs.Created.Before(*end) {
				at := make(map[string]interface{})

				at[ObjAttrName] = attrs.Name
				at[ObjAttrContentType] = attrs.ContentType
				at[ObjAttrOwner] = attrs.Owner
				at[ObjAttrSize] = attrs.Size
				at[ObjAttrContentEncoding] = attrs.ContentEncoding
				at[ObjAttrCreated] = attrs.Created.Unix()

				select {
				case <-ctx.Done():
					return
				default:
					result <- at
				}
			}
		}
	}

	wg.Add(1)
	go rfn(bucketName, prefix)

	go func() {
		wg.Wait()
		close(result)
	}()

	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "ListBucketByTime", "info", "end")
	}

	return result, nil
}

//RemoveFile deletes a bucket file
func (sto *StorMgr) RemoveFile(ctx context.Context, bucketName string, fileName string) error {
	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "RemoveFile", "info", "start")
	}

	err := sto.st.Bucket(bucketName).Object(fileName).Delete(ctx)
	if err != nil {
		return err
	}

	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "RemoveFile", "info", "end")
	}

	return nil
}

//DrainFn drains a channel until it is closed
func DrainFn(c <-chan interface{}) {
	for {
		select {
		case _, ok := <-c:
			if ok == false {
				return
			}
		}
	}
}
