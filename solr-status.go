/*
 * solr-status.go - simple collectd plugin for Apache Solr
 * Copyright (c) 2018 Matteo Fascoli <matteo@fascoli.com>
 */

package main

import (
	"flag"
	"fmt"
	"io/ioutil"
	"log"
	"net/http"
	"os"
	"strconv"
	"strings"
	"time"

	"github.com/Jeffail/gabs"
)

const defaultIntervalSecs = 20
const httpTimeoutSecs = 5
const pluginName = "solr_status"

type SolrStatus struct {
	NumDocs          int
	DeletedDocs      int
	SegmentCount     int
	SizeInBytes      int
	MergeThreadCount int
}

var (
	solrServer = flag.String("server", "", "the solr server we need to poll")
	coreName   = flag.String("core", "", "the core name we want to get data from")
	useHTTPS   = flag.Bool("https", false, "use HTTPS while connecting to the solr server")
)

func main() {

	// Process parameters.
	flag.Parse()
	if *solrServer == "" {
		fmt.Println("no solr server specified. Exiting.")
		os.Exit(1)
	}
	if *coreName == "" {
		fmt.Println("no core name specified. Exiting.")
		os.Exit(1)
	}

	// get hostname from ENV.
	hostname := os.Getenv("COLLECTD_HOSTNAME")
	if len(hostname) == 0 {
		hostname = "localhost"
	}

	// Get check interval from ENV.
	interval, err := strconv.ParseInt(os.Getenv("COLLECTD_INTERVAL"), 10, 32)
	if err != nil {
		interval = defaultIntervalSecs
	}

	// Fetch data from the specified server/core.
	var status SolrStatus

	for {
		err := getStatus(*coreName, &status)
		if err != nil {
			log.Println(err)
			time.Sleep(time.Second * time.Duration(interval))
			continue
		}

		// Use os.Stdout so that the output is not buffered.
		now := time.Now().Unix()
		fmt.Fprintf(os.Stdout, "PUTVAL %s/%s/gauge-numdocs %d:%d\n",
			hostname,
			pluginName,
			now,
			status.NumDocs)
		fmt.Fprintf(os.Stdout, "PUTVAL %s/%s/gauge-deleteddocs %d:%d\n",
			hostname,
			pluginName,
			now,
			status.DeletedDocs)
		fmt.Fprintf(os.Stdout, "PUTVAL %s/%s/gauge-segmentcount %d:%d\n",
			hostname,
			pluginName,
			now,
			status.SegmentCount)
		fmt.Fprintf(os.Stdout, "PUTVAL %s/%s/gauge-sizeinbytes %d:%d\n",
			hostname,
			pluginName,
			now,
			status.SizeInBytes)

		fmt.Fprintf(os.Stdout, "PUTVAL %s/%s/gauge-mergethreadcount %d:%d\n",
			hostname,
			pluginName,
			now,
			status.MergeThreadCount)

		time.Sleep(time.Second * time.Duration(interval))
	}
}

// Get an int value from a gabs query. Returns 0 if not found.
func getGabsInt(core, key string, gabs *gabs.Container) int {
	value, ok := gabs.Path("status." + core + ".index." + key).Data().(float64)

	if ok {
		return int(value)
	} else {
		return 0
	}

}

// Query the specified Solr server and extract the relevant stats.
func getStatus(core string, status *SolrStatus) error {

	var prefix string
	if *useHTTPS {
		prefix = "https"
	} else {
		prefix = "http"
	}

	var coreUrl = fmt.Sprintf("%s://%s/solr/admin/cores?action=STATUS&core=%s&wt=json",
		prefix,
		*solrServer,
		*coreName)

	// Fetch core-specific stats.
	data, err := getParsedJson(coreUrl)
	if err != nil {
		return err
	}

	// Verify if we can pull data (since Solr won't generate an error if the core does not exist).
	// Then, collect the core's data we are interested in.
	if data.Path("status."+core+".name").String() != fmt.Sprintf("\"%s\"", core) {
		return fmt.Errorf("no data could be found for the index '%s'", core)
	} else {
		status.NumDocs = getGabsInt(core, "numDocs", data)
		status.DeletedDocs = getGabsInt(core, "deletedDocs", data)
		status.SegmentCount = getGabsInt(core, "segmentCount", data)
		status.SizeInBytes = getGabsInt(core, "sizeInBytes", data)
	}

	// Fetch server-wide stats.
	var serverUrl = fmt.Sprintf("%s://%s/solr/admin/info/threads", prefix, *solrServer)
	data, err = getParsedJson(serverUrl)
	if err != nil {
		return err
	}

	// Count how many "Lucene Merge Thread" are listed.
	mergeThreadCount := 0
	for _, child := range data.S("system", "threadDump").Children() {
		cm := child.ChildrenMap()
		if strings.HasPrefix(strings.Trim(cm["name"].String(), "\""), "Lucene Merge Thread") {
			mergeThreadCount += 1
		}
	}
	status.MergeThreadCount = mergeThreadCount

	return nil
}

// Query the specified URL and return the body.
func getParsedJson(url string) (*gabs.Container, error) {
	var httpClient = &http.Client{Timeout: httpTimeoutSecs * time.Second}

	r, err := httpClient.Get(url)
	if err != nil {
		return nil, fmt.Errorf("cannot fetch url: %v", err)
	}
	defer r.Body.Close()

	if r.StatusCode != http.StatusOK {
		return nil, fmt.Errorf("server did not reply as expected: got status code %d, expected 200",
			r.StatusCode)
	}

	body, err := ioutil.ReadAll(r.Body)
	if err != nil {
		return nil, fmt.Errorf("cannot read respose: %v", err)
	}

	data, err := gabs.ParseJSON(body)
	if err != nil {
		return nil, fmt.Errorf("cannot parse json reply: %v", err)
	}

	return data, nil
}
