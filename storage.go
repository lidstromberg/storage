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
	localStClient *storage.Client
	bc            lbcf.ConfigSetting
}

//NewStorMgr returns a new storage manager
func NewStorMgr(ctx context.Context, bc lbcf.ConfigSetting) (*StorMgr, error) {
	preflight(ctx, bc)

	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "NewMgr", "info", "start")
	}

	storageClient := &storage.Client{}

	if bc.GetConfigValue(ctx, "EnvStorCrdFile") != "" {
		sc, err := storage.NewClient(ctx, option.WithGRPCConnectionPool(EnvClientPool), option.WithCredentialsFile(bc.GetConfigValue(ctx, "EnvStorCrdFile")))
		if err != nil {
			return nil, err
		}
		storageClient = sc
	} else {
		sc, err := storage.NewClient(ctx, option.WithGRPCConnectionPool(EnvClientPool))
		if err != nil {
			return nil, err
		}
		storageClient = sc
	}

	st1 := &StorMgr{
		localStClient: storageClient,
		bc:            bc,
	}

	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "NewMgr", "info", "end")
	}

	return st1, nil
}

//GetBucketFileData returns a byte array for a bucket file
func (sto *StorMgr) GetBucketFileData(ctx context.Context, bucketName string, fileName string) ([]byte, error) {
	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "GetBucketFileData", "info", "start")
	}

	rc, err := sto.localStClient.Bucket(bucketName).Object(fileName).NewReader(ctx)
	if err != nil {
		return nil, err
	}
	defer rc.Close()

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
func (sto *StorMgr) WriteBucketFile(ctx context.Context, bucketName string, contentType, fileName string, data []byte) error {
	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "WriteBucketFile", "info", "start")
	}

	wc := sto.localStClient.Bucket(bucketName).Object(fileName).NewWriter(ctx)
	defer wc.Close()

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
func (sto *StorMgr) ListBucket(ctx context.Context, bucketName, prefix string, start, end *time.Time, bufferSize int) (<-chan interface{}, error) {

	if EnvDebugOn {
		lblog.LogEvent("StorMgr", "ListBucket", "info", "start")
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

			itx := sto.localStClient.Bucket(lbucketName).Objects(ctx, qr)
			it = itx
		} else {
			itx := sto.localStClient.Bucket(lbucketName).Objects(ctx, nil)
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

	return result, nil
}
