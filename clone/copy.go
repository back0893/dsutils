package clone

import (
	"context"
	"fmt"
	"net/url"
	"os"
	"sync"
	"time"

	ds "github.com/ipfs/go-datastore"
	"github.com/ipfs/go-datastore/query"
	badger "github.com/ipfs/go-ds-badger"
	logger "github.com/ipfs/go-log/v2"
	mongods "github.com/textileio/go-ds-mongo"
)

var (
	log = logger.Logger("dsutils")
)

func CloneFromRemoteToLocal(
	fromMongoUri string,
	fromMongoDatabase string,
	fromMongoCollection string,
	toBadgerRepo string,
	parallel int,
	verbose bool,
) (ds.TxnDatastore, error) {
	return clone("", fromMongoUri, fromMongoDatabase, fromMongoCollection, toBadgerRepo, "", "", "", parallel, verbose)
}

func clone(
	fromBadgerRepo string,
	fromMongoUri string,
	fromMongoDatabase string,
	fromMongoCollection string,

	toBadgerRepo string,
	toMongoUri string,
	toMongoDatabase string,
	toMongoCollection string,

	parallel int,
	verbose bool,
) (ds.TxnDatastore, error) {
	if len(fromBadgerRepo) != 0 && len(fromMongoUri) != 0 {
		return nil, fmt.Errorf("multiple sources specified")
	}
	if len(fromBadgerRepo) == 0 && len(fromMongoUri) == 0 {
		return nil, fmt.Errorf("source not specified")
	}
	if len(toBadgerRepo) != 0 && len(toMongoUri) != 0 {
		return nil, fmt.Errorf("multiple destinations specified")
	}
	if len(toBadgerRepo) == 0 && len(toMongoUri) == 0 {
		return nil, fmt.Errorf("destination not specified")
	}

	var from, to ds.TxnDatastore
	var err error
	if len(fromBadgerRepo) != 0 {
		from, err = badger.NewDatastore(fromBadgerRepo, &badger.DefaultOptions)
		if err != nil {
			return nil, fmt.Errorf("connecting to badger source: %v", err)
		}
		log.Infof("connected to badger source: %s", fromBadgerRepo)
	}
	if len(toBadgerRepo) != 0 {
		if err := os.MkdirAll(toBadgerRepo, os.ModePerm); err != nil {
			return nil, fmt.Errorf("making destination path: %v", err)
		}
		to, err = badger.NewDatastore(toBadgerRepo, &badger.DefaultOptions)
		if err != nil {
			return nil, fmt.Errorf("connecting to badger destination: %v", err)
		}
		log.Infof("connected to badger destination: %s", toBadgerRepo)
	}

	ctx, cancel := context.WithCancel(context.Background())
	defer cancel()
	if len(fromMongoUri) != 0 {
		uri, err := url.Parse(fromMongoUri)
		if err != nil {
			return nil, fmt.Errorf("parsing source mongo URI: %v", err)
		}
		if len(fromMongoDatabase) == 0 {
			return nil, fmt.Errorf("source mongo database not specified")
		}
		from, err = mongods.New(ctx, fromMongoUri, fromMongoDatabase, mongods.WithCollName(fromMongoCollection))
		if err != nil {
			return nil, fmt.Errorf("connecting to mongo source: %v", err)
		}
		log.Infof("connected to mongo source: %s", uri.Redacted())
	}
	if len(toMongoUri) != 0 {
		uri, err := url.Parse(toMongoUri)
		if err != nil {
			return nil, fmt.Errorf("parsing destination mongo URI: %v", err)
		}
		if len(toMongoDatabase) == 0 {
			return nil, fmt.Errorf("destination mongo database not specified")
		}
		to, err = mongods.New(ctx, toMongoUri, toMongoDatabase, mongods.WithCollName(toMongoCollection))
		if err != nil {
			return nil, fmt.Errorf("connecting to mongo destination: %v", err)
		}
		log.Infof("connected to mongo destination: %s", uri.Redacted())
	}

	res, err := from.Query(query.Query{})
	if err != nil {
		return nil, fmt.Errorf("querying source: %v", err)
	}
	defer res.Close()

	var lock sync.Mutex
	var errors []string
	var count int
	start := time.Now()
	lim := make(chan struct{}, parallel)
	for r := range res.Next() {
		if r.Error != nil {
			return nil, fmt.Errorf("getting next source result: %v", r.Error)
		}
		lim <- struct{}{}

		r := r
		go func() {
			defer func() { <-lim }()

			if err := to.Put(ds.NewKey(r.Key), r.Value); err != nil {
				lock.Lock()
				errors = append(errors, fmt.Sprintf("copying %s: %v", r.Key, err))
				lock.Unlock()
				return
			}
			if verbose {
				log.Infof("copied %s", r.Key)
			}
			lock.Lock()
			count++
			if count%parallel == 0 {
				log.Infof("copied %d keys", count)
			}
			lock.Unlock()
		}()
	}
	for i := 0; i < cap(lim); i++ {
		lim <- struct{}{}
	}

	if len(errors) > 0 {
		for _, m := range errors {
			log.Error(m)
		}
		return nil, fmt.Errorf("had %d errors", len(errors))
	}

	log.Infof("copied %d keys in %s", count, time.Since(start))

	return to, nil
}
