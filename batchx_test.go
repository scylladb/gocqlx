// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

//go:build all || integration
// +build all integration

package gocqlx_test

import (
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"
	"github.com/scylladb/gocqlx/v2"
	"github.com/scylladb/gocqlx/v2/gocqlxtest"
	"github.com/scylladb/gocqlx/v2/qb"
)

func TestBatch(t *testing.T) {
	t.Parallel()

	cluster := gocqlxtest.CreateCluster()
	if err := gocqlxtest.CreateKeyspace(cluster, "batch_test"); err != nil {
		t.Fatal("create keyspace:", err)
	}

	session, err := gocqlx.WrapSession(cluster.CreateSession())
	if err != nil {
		t.Fatal("create session:", err)
	}
	t.Cleanup(func() {
		session.Close()
	})

	basicCreateAndPopulateKeyspace(t, session, "batch_test")

	song := Song{
		ID:     mustParseUUID("60fc234a-8481-4343-93bb-72ecab404863"),
		Title:  "La Petite Tonkinoise",
		Album:  "Bye Bye Blackbird",
		Artist: "Joséphine Baker",
		Tags:   []string{"jazz"},
		Data:   []byte("music"),
	}
	playlist := PlaylistItem{
		ID:     mustParseUUID("6a6255d9-680f-4cb5-b9a2-27cf4a810344"),
		Title:  "La Petite Tonkinoise",
		Album:  "Bye Bye Blackbird",
		Artist: "Joséphine Baker",
		SongID: mustParseUUID("60fc234a-8481-4343-93bb-72ecab404863"),
	}

	insertSong := qb.Insert("batch_test.songs").
		Columns("id", "title", "album", "artist", "tags", "data").Query(session)
	insertPlaylist := qb.Insert("batch_test.playlists").
		Columns("id", "title", "album", "artist", "song_id").Query(session)
	selectSong := qb.Select("batch_test.songs").Where(qb.Eq("id")).Query(session)
	selectPlaylist := qb.Select("batch_test.playlists").Where(qb.Eq("id")).Query(session)

	t.Run("batch inserts", func(t *testing.T) {
		t.Parallel()

		type batchQry struct {
			qry *gocqlx.Queryx
			arg interface{}
		}

		qrys := []batchQry{
			{qry: insertSong, arg: song},
			{qry: insertPlaylist, arg: playlist},
		}

		b := session.NewBatch(gocql.LoggedBatch)
		for _, qry := range qrys {
			if err := b.BindStruct(qry.qry, qry.arg); err != nil {
				t.Fatal("BindStruct failed:", err)
			}
		}
		if err := session.ExecuteBatch(b); err != nil {
			t.Fatal("batch execution:", err)
		}

		// verify song was inserted
		var gotSong Song
		if err := selectSong.BindStruct(song).Get(&gotSong); err != nil {
			t.Fatal("select song:", err)
		}
		if diff := cmp.Diff(gotSong, song); diff != "" {
			t.Errorf("expected %v song, got %v, diff: %q", song, gotSong, diff)
		}

		// verify playlist item was inserted
		var gotPlayList PlaylistItem
		if err := selectPlaylist.BindStruct(playlist).Get(&gotPlayList); err != nil {
			t.Fatal("select song:", err)
		}
		if diff := cmp.Diff(gotPlayList, playlist); diff != "" {
			t.Errorf("expected %v playList, got %v, diff: %q", playlist, gotPlayList, diff)
		}
	})
}
