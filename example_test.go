// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

// +build all integration

package gocqlx_test

import (
	"fmt"
	"math"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/gocqlxtest"
	"github.com/scylladb/gocqlx/v2/qb"
	"github.com/scylladb/gocqlx/v2/table"
	"golang.org/x/sync/errgroup"
)

// Running examples locally:
// make run-scylla
// make run-examples
func TestExample(t *testing.T) {
	cluster := gocqlxtest.CreateCluster()

	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		t.Fatal("create session:", err)
	}
	defer session.Close()

	session.ExecStmt(`DROP KEYSPACE examples`)

	basicCreateAndPopulateKeyspace(t, session)
	basicReadScyllaVersion(t, session)

	datatypesBlob(t, session)
	datatypesUserDefinedType(t, session)
	datatypesUserDefinedTypeWrapper(t, session)
	datatypesJson(t, session)

	pagingForwardPaging(t, session)
	pagingEfficientFullTableScan(t, session)

	lwtLock(t, session)
}

// This example shows how to use query builders and table models to build
// queries. It uses "BindStruct" function for parameter binding and "Select"
// function for loading data to a slice.
func basicCreateAndPopulateKeyspace(t *testing.T, session gocqlx.Session) {
	err := session.ExecStmt(`CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`)
	if err != nil {
		t.Fatal("create keyspace:", err)
	}

	type Song struct {
		ID     gocql.UUID
		Title  string
		Album  string
		Artist string
		Tags   []string
		Data   []byte
	}

	type PlaylistItem struct {
		ID     gocql.UUID
		Title  string
		Album  string
		Artist string
		SongID gocql.UUID
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS examples.songs (
		id uuid PRIMARY KEY,
		title text,
		album text,
		artist text,
		tags set<text>,
		data blob)`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS examples.playlists (
		id uuid,
		title text,
		album text, 
		artist text,
		song_id uuid,
		PRIMARY KEY (id, title, album, artist))`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	playlistMetadata := table.Metadata{
		Name:    "examples.playlists",
		Columns: []string{"id", "title", "album", "artist", "song_id"},
		PartKey: []string{"id"},
		SortKey: []string{"title", "album", "artist", "song_id"},
	}
	playlistTable := table.New(playlistMetadata)

	// Insert song using query builder.
	stmt, names := qb.Insert("examples.songs").
		Columns("id", "title", "album", "artist", "tags", "data").ToCql()
	insertSong := session.Query(stmt, names)

	insertSong.BindStruct(Song{
		ID:     mustParseUUID("756716f7-2e54-4715-9f00-91dcbea6cf50"),
		Title:  "La Petite Tonkinoise",
		Album:  "Bye Bye Blackbird",
		Artist: "Joséphine Baker",
		Tags:   []string{"jazz", "2013"},
		Data:   []byte("music"),
	})
	if err := insertSong.ExecRelease(); err != nil {
		t.Fatal("ExecRelease() failed:", err)
	}

	// Insert playlist using table model.
	insertPlaylist := session.Query(playlistTable.Insert())

	insertPlaylist.BindStruct(PlaylistItem{
		ID:     mustParseUUID("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
		Title:  "La Petite Tonkinoise",
		Album:  "Bye Bye Blackbird",
		Artist: "Joséphine Baker",
		SongID: mustParseUUID("756716f7-2e54-4715-9f00-91dcbea6cf50"),
	})
	if err := insertPlaylist.ExecRelease(); err != nil {
		t.Fatal("ExecRelease() failed:", err)
	}

	// Query and displays data.
	queryPlaylist := session.Query(playlistTable.Select())

	queryPlaylist.BindStruct(&PlaylistItem{
		ID: mustParseUUID("2cc9ccb7-6221-4ccb-8387-f22b6a1b354d"),
	})

	var items []*PlaylistItem
	if err := queryPlaylist.Select(&items); err != nil {
		t.Fatal("Select() failed:", err)
	}

	for _, i := range items {
		t.Logf("%+v", *i)
	}
}

// This example shows how to load a single value using "Get" function.
// Get can also work with UDTs and types that implement gocql marshalling functions.
func basicReadScyllaVersion(t *testing.T, session gocqlx.Session) {
	var releaseVersion string

	err := session.Query("SELECT release_version FROM system.local", nil).Get(&releaseVersion)
	if err != nil {
		t.Fatal("Get() failed:", err)
	}

	t.Logf("Scylla version is: %s", releaseVersion)
}

// This examples shows how to bind data from a map using "BindMap" function,
// override field name mapping using the "db" tags, and use "Unsafe" function
// to handle situations where driver returns more coluns that we are ready to
// consume.
func datatypesBlob(t *testing.T, session gocqlx.Session) {
	err := session.ExecStmt(`CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`)
	if err != nil {
		t.Fatal("create keyspace:", err)
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS examples.blobs(k int PRIMARY KEY, b blob, m map<text, blob>)`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	// One way to get a byte buffer is to allocate it and fill it yourself:
	var buf [16]byte
	for i := range buf {
		buf[i] = 0xff
	}

	insert := session.Query(qb.Insert("examples.blobs").Columns("k", "b", "m").ToCql())
	insert.BindMap(qb.M{
		"k": 1,
		"b": buf[:],
		"m": map[string][]byte{"test": buf[:]},
	})

	if err := insert.ExecRelease(); err != nil {
		t.Fatal("ExecRelease() failed:", err)
	}

	row := &struct {
		Buffer  []byte            `db:"b"`
		Mapping map[string][]byte `db:"m"`
	}{}
	q := session.Query(qb.Select("examples.blobs").Where(qb.EqLit("k", "1")).ToCql())

	// Unsafe is used here to override validation error that check if all
	// requested columns are consumed `failed: missing destination name "k" in struct` error
	if err := q.Iter().Unsafe().Get(row); err != nil {
		t.Fatal("Get() failed:", err)
	}

	t.Logf("%+v", row.Buffer)
	t.Logf("%+v", row.Mapping)
}

type Coordinates struct {
	gocqlx.UDT
	X int
	Y int
}

// This example shows how to add User Defined Type marshalling capabilities by
// adding a single line - embedding gocqlx.UDT.
func datatypesUserDefinedType(t *testing.T, session gocqlx.Session) {
	err := session.ExecStmt(`CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`)
	if err != nil {
		t.Fatal("create keyspace:", err)
	}

	err = session.ExecStmt(`CREATE TYPE IF NOT EXISTS examples.coordinates(x int, y int)`)
	if err != nil {
		t.Fatal("create type:", err)
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS examples.udts(k int PRIMARY KEY, c coordinates)`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	coordinates1 := Coordinates{X: 12, Y: 34}
	coordinates2 := Coordinates{X: 56, Y: 78}

	insert := session.Query(qb.Insert("examples.udts").Columns("k", "c").ToCql())
	insert.BindMap(qb.M{
		"k": 1,
		"c": coordinates1,
	})
	if err := insert.Exec(); err != nil {
		t.Fatal("Exec() failed:", err)
	}
	insert.BindMap(qb.M{
		"k": 2,
		"c": coordinates2,
	})
	if err := insert.Exec(); err != nil {
		t.Fatal("Exec() failed:", err)
	}

	var coordinates []Coordinates
	q := session.Query(qb.Select("examples.udts").Columns("c").ToCql())
	if err := q.Select(&coordinates); err != nil {
		t.Fatal("Select() failed:", err)
	}

	for _, c := range coordinates {
		t.Logf("%+v", c)
	}
}

type coordinates struct {
	X int
	Y int
}

// This example shows how to add User Defined Type marshalling capabilities to
// types that we cannot modify, like library or transfer objects, without
// rewriting them in runtime.
func datatypesUserDefinedTypeWrapper(t *testing.T, session gocqlx.Session) {
	err := session.ExecStmt(`CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`)
	if err != nil {
		t.Fatal("create keyspace:", err)
	}

	err = session.ExecStmt(`CREATE TYPE IF NOT EXISTS examples.coordinates(x int, y int)`)
	if err != nil {
		t.Fatal("create type:", err)
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS examples.udts_wrapper(k int PRIMARY KEY, c coordinates)`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	// Embed coordinates within CoordinatesUDT
	c1 := &coordinates{X: 12, Y: 34}
	c2 := &coordinates{X: 56, Y: 78}

	type CoordinatesUDT struct {
		gocqlx.UDT
		*coordinates
	}

	coordinates1 := CoordinatesUDT{coordinates: c1}
	coordinates2 := CoordinatesUDT{coordinates: c2}

	insert := session.Query(qb.Insert("examples.udts_wrapper").Columns("k", "c").ToCql())
	insert.BindMap(qb.M{
		"k": 1,
		"c": coordinates1,
	})
	if err := insert.Exec(); err != nil {
		t.Fatal("Exec() failed:", err)
	}
	insert.BindMap(qb.M{
		"k": 2,
		"c": coordinates2,
	})
	if err := insert.Exec(); err != nil {
		t.Fatal("Exec() failed:", err)
	}

	var coordinates []Coordinates
	q := session.Query(qb.Select("examples.udts_wrapper").Columns("c").ToCql())
	if err := q.Select(&coordinates); err != nil {
		t.Fatal("Select() failed:", err)
	}

	for _, c := range coordinates {
		t.Logf("%+v", c)
	}
}

// This example shows how to use query builder to work with
func datatypesJson(t *testing.T, session gocqlx.Session) {
	err := session.ExecStmt(`CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`)
	if err != nil {
		t.Fatal("create keyspace:", err)
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS examples.querybuilder_json(id int PRIMARY KEY, name text, specs map<text, text>)`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	insert := session.Query(qb.Insert("examples.querybuilder_json").Json().ToCql())

	insert.Bind(`{ "id": 1, "name": "Mouse", "specs": { "color": "silver" } }`)
	if err := insert.Exec(); err != nil {
		t.Fatal("Exec() failed:", err)
	}
	insert.Bind(`{ "id": 2, "name": "Keyboard", "specs": { "layout": "qwerty" } }`)
	if err := insert.Exec(); err != nil {
		t.Fatal("Exec() failed:", err)
	}

	// fromJson lets you provide individual columns as JSON:
	stmt, names := qb.Insert("examples.querybuilder_json").
		Columns("id", "name").
		FuncColumn("specs", qb.Fn("fromJson", "json")).
		ToCql()

	insertFromJson := session.Query(stmt, names)
	insertFromJson.BindMap(qb.M{
		"id":   3,
		"name": "Screen",
		"json": `{ "size": "24-inch" }`,
	})
	if err := insertFromJson.Exec(); err != nil {
		t.Fatal("Exec() failed:", err)
	}

	// Reading the whole row as a JSON object:
	stmt, names = qb.Select("examples.querybuilder_json").
		Json().
		Where(qb.EqLit("id", "1")).
		ToCql()
	q := session.Query(stmt, names)

	var jsonString string

	if err := q.Get(&jsonString); err != nil {
		t.Fatal("Get() failed:", err)
	}
	t.Logf("Entry #1 as JSON: %s", jsonString)

	// Extracting a particular column as JSON:
	stmt, names = qb.Select("examples.querybuilder_json").
		Columns("id", "toJson(specs) AS json_specs").
		Where(qb.EqLit("id", "2")).
		ToCql()
	q = session.Query(stmt, names)

	row := &struct {
		ID        int
		JsonSpecs string
	}{}
	if err := q.Get(row); err != nil {
		t.Fatal("Get() failed:", err)
	}
	t.Logf("Entry #%d's specs as JSON: %s", row.ID, row.JsonSpecs)
}

type Video struct {
	UserID   int
	UserName string
	Added    time.Time
	VideoID  int
	Title    string
}

func pagingFillTable(t *testing.T, insert *gocqlx.Queryx) {
	t.Helper()

	// 3 users
	for i := 0; i < 3; i++ {
		// 49 videos each
		for j := 0; j < 49; j++ {
			insert.BindStruct(Video{
				UserID:   i,
				UserName: fmt.Sprint("user ", i),
				Added:    time.Unix(int64(j)*100, 0),
				VideoID:  i*100 + j,
				Title:    fmt.Sprint("video ", i*100+j),
			})

			if err := insert.Exec(); err != nil {
				t.Fatal("Exec() failed:", err)
			}
		}
	}
}

// This example shows how to use stateful paging and how "Select" function
// can be used to fetch single page only.
func pagingForwardPaging(t *testing.T, session gocqlx.Session) {
	err := session.ExecStmt(`CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`)
	if err != nil {
		t.Fatal("create keyspace:", err)
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS examples.paging_forward_paging(
		user_id int,
		user_name text,
		added timestamp,
		video_id int,
		title text,
		PRIMARY KEY (user_id, added, video_id)
	) WITH CLUSTERING ORDER BY (added DESC, video_id ASC)`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	videoMetadata := table.Metadata{
		Name:    "examples.paging_forward_paging",
		Columns: []string{"user_id", "user_name", "added", "video_id", "title"},
		PartKey: []string{"user_id"},
		SortKey: []string{"added", "video_id"},
	}
	videoTable := table.New(videoMetadata)

	pagingFillTable(t, session.Query(videoTable.Insert()))

	// Query and displays data. Iterate over videos of user "1" 10 entries per request.

	const itemsPerPage = 10

	getUserVideos := func(userID int, page []byte) (userVideos []Video, nextPage []byte, err error) {
		q := session.Query(videoTable.Select()).Bind(userID)
		defer q.Release()
		q.PageState(page)
		q.PageSize(itemsPerPage)

		iter := q.Iter()
		return userVideos, iter.PageState(), iter.Select(&userVideos)
	}

	var (
		userVideos []Video
		nextPage   []byte
	)

	for i := 1; ; i++ {
		userVideos, nextPage, err = getUserVideos(1, nextPage)
		if err != nil {
			t.Fatalf("oad page %d: %s", i, err)
		}

		t.Logf("Page %d:", i)
		for _, v := range userVideos {
			t.Logf("%+v", v)
		}
		if len(nextPage) == 0 {
			break
		}
	}
}

// This example shows how to efficiently process all rows in a table using
// the "token" function. It implements idea from blog post [1]:
// As a bonus we use "CompileNamedQueryString" to get named parameters out of
// CQL query placeholders like in Python or Java driver.
//
// [1] https://www.scylladb.com/2017/02/13/efficient-full-table-scans-with-scylla-1-6/.
func pagingEfficientFullTableScan(t *testing.T, session gocqlx.Session) {
	err := session.ExecStmt(`CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`)
	if err != nil {
		t.Fatal("create keyspace:", err)
	}

	err = session.ExecStmt(`CREATE TABLE IF NOT EXISTS examples.paging_efficient_full_table_scan(
		user_id int,
		user_name text,
		added timestamp,
		video_id int,
		title text,
		PRIMARY KEY (user_id, added, video_id)
	) WITH CLUSTERING ORDER BY (added DESC, video_id ASC)`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	videoMetadata := table.Metadata{
		Name:    "examples.paging_efficient_full_table_scan",
		Columns: []string{"user_id", "user_name", "added", "video_id", "title"},
		PartKey: []string{"user_id"},
		SortKey: []string{"added", "video_id"},
	}
	videoTable := table.New(videoMetadata)

	pagingFillTable(t, session.Query(videoTable.Insert()))

	// Calculate optimal number of workers for the cluster:
	var (
		nodesInCluster = 1
		coresInNode    = 1
		smudgeFactor   = 3
	)
	workers := nodesInCluster * coresInNode * smudgeFactor

	t.Logf("Workers %d", workers)

	type tokenRange struct {
		Start int64
		End   int64
	}
	buf := make(chan tokenRange)

	// sequencer pushes token ranges to buf
	sequencer := func() error {
		span := int64(math.MaxInt64 / (50 * workers))

		tr := tokenRange{math.MinInt64, math.MinInt64 + span}
		for tr.End > tr.Start {
			buf <- tr
			tr.Start = tr.End
			tr.End += span
		}

		tr.End = math.MaxInt64
		buf <- tr
		close(buf)

		return nil
	}

	// worker queries a token ranges generated by sequencer
	worker := func() error {
		const cql = `SELECT * FROM examples.paging_efficient_full_table_scan WHERE 
			token(user_id) >= :start AND 
			token(user_id) < :end`

		stmt, names, err := gocqlx.CompileNamedQueryString(cql)
		if err != nil {
			return err
		}
		q := session.Query(stmt, names)
		defer q.Release()

		var v Video
		for {
			tr, ok := <-buf
			if !ok {
				break
			}

			iter := q.BindStruct(tr).Iter()
			for iter.StructScan(&v) {
				t.Logf("%+v:", v)
			}
			if err := iter.Close(); err != nil {
				return err
			}
		}

		return nil
	}

	// Query and displays data.

	var wg errgroup.Group
	wg.Go(sequencer)
	for i := 0; i < workers; i++ {
		wg.Go(worker)
	}

	if err := wg.Wait(); err != nil {
		t.Fatal(err)
	}
}

// This example shows how to use Lightweight Transactions (LWT) aka.
// Compare-And-Set (CAS) functions.
// See: https://docs.scylladb.com/using-scylla/lwt/ for more details.
func lwtLock(t *testing.T, session gocqlx.Session) {
	err := session.ExecStmt(`CREATE KEYSPACE IF NOT EXISTS examples WITH replication = {'class': 'SimpleStrategy', 'replication_factor': 1}`)
	if err != nil {
		t.Fatal("create keyspace:", err)
	}

	type Lock struct {
		Name  string
		Owner string
		TTL   int64
	}

	err = session.ExecStmt(`CREATE TABLE examples.lock (name text PRIMARY KEY, owner text)`)
	if err != nil {
		t.Fatal("create table:", err)
	}

	extend := func(lock Lock) bool {
		q := session.Query(qb.Update("examples.lock").
			Set("owner").
			Where(qb.Eq("name")).
			If(qb.Eq("owner")).
			TTLNamed("ttl").
			ToCql())
		q.BindStruct(lock)

		applied, err := q.ExecCASRelease()
		if err != nil {
			t.Fatal("ExecCASRelease() failed:", err)
		}
		return applied
	}

	acquire := func(lock Lock) (applied bool) {
		var prev Lock

		defer func() {
			t.Logf("Acquire %+v applied %v owner %+v)", lock, applied, prev)
		}()

		q := session.Query(qb.Insert("examples.lock").
			Columns("name", "owner").
			TTLNamed("ttl").
			Unique().
			ToCql(),
		)
		q.BindStruct(lock)

		applied, err = q.GetCASRelease(&prev)
		if err != nil {
			t.Fatal("GetCASRelease() failed:", err)
		}
		if applied {
			return true
		}
		if prev.Owner == lock.Owner {
			return extend(lock)
		}
		return false
	}

	const (
		resource = "acme"
		ttl      = time.Second
	)

	l1 := Lock{
		Name:  resource,
		Owner: "1",
		TTL:   qb.TTL(ttl),
	}

	l2 := Lock{
		Name:  resource,
		Owner: "2",
		TTL:   qb.TTL(ttl),
	}

	if !acquire(l1) {
		t.Fatal("l1 failed to acquire lock")
	}
	if acquire(l2) {
		t.Fatal("unexpectedly l2 acquired lock")
	}
	if !acquire(l1) {
		t.Fatal("l1 failed to extend lock")
	}
	time.Sleep(time.Second)
	if !acquire(l2) {
		t.Fatal("l2 failed to acquire lock")
	}
	if acquire(l1) {
		t.Fatal("unexpectedly l1 acquired lock")
	}
}

func mustParseUUID(s string) gocql.UUID {
	u, err := gocql.ParseUUID(s)
	if err != nil {
		panic(err)
	}
	return u
}
