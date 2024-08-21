package schemagentest

import (
	"flag"
	"strings"
	"testing"
	"time"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/qb"
)

var flagCluster = flag.String("cluster", "127.0.0.1", "a comma-separated list of host:port or host tuples")

func TestModelLoad(t *testing.T) {
	session, err := gocqlx.WrapSession(gocql.NewCluster(strings.Split(*flagCluster, ",")...).CreateSession())
	if err != nil {
		t.Fatal("create session:", err.Error())
	}
	defer session.Close()

	// Keyspace, types and table are created at `schemaget_test.go` at `createTestSchema`

	song := SongsStruct{
		Id:       gocql.TimeUUID(),
		Title:    "title",
		Album:    "album",
		Artist:   "artist",
		Duration: gocql.Duration{Nanoseconds: int64(5 * time.Minute)},
		Tags:     []string{"tag1", "tag2"},
		Data:     []byte("data"),
	}

	err = qb.Insert("schemagen.songs").
		Columns("id", "title", "album", "artist", "duration", "tags", "data").
		Query(session).
		BindStruct(&song).
		Exec()
	if err != nil {
		t.Fatal("failed to insert song:", err.Error())
	}

	loadedSong := SongsStruct{}
	err = qb.Select("schemagen.songs").
		Columns("id", "title", "album", "artist", "duration", "tags", "data").
		Where(qb.Eq("id")).
		Query(session).
		BindMap(map[string]interface{}{"id": song.Id}).
		Get(&loadedSong)
	if err != nil {
		t.Fatal("failed to select song:", err)
	}
	if diff := cmp.Diff(song, loadedSong); diff != "" {
		t.Error("loaded song is different from inserted song:", diff)
	}

	pl := PlaylistsStruct{
		Id:     gocql.TimeUUID(),
		Title:  "title",
		Album:  AlbumUserType{Name: "album", Songwriters: []string{"songwriter1", "songwriter2"}},
		Artist: "artist",
		SongId: gocql.TimeUUID(),
	}

	err = qb.Insert("schemagen.playlists").
		Columns("id", "title", "album", "artist", "song_id").
		Query(session).
		BindStruct(&pl).
		Exec()
	if err != nil {
		t.Fatal("failed to insert playlist:", err.Error())
	}

	loadedPl := PlaylistsStruct{}

	err = qb.Select("schemagen.playlists").
		Columns("id", "title", "album", "artist", "song_id").
		Where(qb.Eq("id")).
		Query(session).
		BindMap(map[string]interface{}{"id": pl.Id}).
		Get(&loadedPl)
	if err != nil {
		t.Fatal("failed to select playlist:", err.Error())
	}

	if diff := cmp.Diff(pl, loadedPl); diff != "" {
		t.Error("loaded playlist is different from inserted song:", diff)
	}
}
