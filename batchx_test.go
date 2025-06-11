// Copyright (C) 2017 ScyllaDB
// Use of this source code is governed by a ALv2-style
// license that can be found in the LICENSE file.

//go:build all || integration
// +build all integration

package gocqlx_test

import (
	"reflect"
	"testing"

	"github.com/gocql/gocql"
	"github.com/google/go-cmp/cmp"

	"github.com/scylladb/gocqlx/v3"
	"github.com/scylladb/gocqlx/v3/gocqlxtest"
	"github.com/scylladb/gocqlx/v3/qb"
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

	t.Run("batch inserts", func(t *testing.T) {
		t.Parallel()

		tcases := []struct {
			name           string
			methodSong     func(*gocqlx.Batch, *gocqlx.Queryx, Song) error
			methodPlaylist func(*gocqlx.Batch, *gocqlx.Queryx, PlaylistItem) error
		}{
			{
				name: "BindStruct",
				methodSong: func(b *gocqlx.Batch, q *gocqlx.Queryx, song Song) error {
					return b.BindStruct(q, song)
				},
				methodPlaylist: func(b *gocqlx.Batch, q *gocqlx.Queryx, playlist PlaylistItem) error {
					return b.BindStruct(q, playlist)
				},
			},
			{
				name: "BindMap",
				methodSong: func(b *gocqlx.Batch, q *gocqlx.Queryx, song Song) error {
					return b.BindMap(q, map[string]interface{}{
						"id":     song.ID,
						"title":  song.Title,
						"album":  song.Album,
						"artist": song.Artist,
						"tags":   song.Tags,
						"data":   song.Data,
					})
				},
				methodPlaylist: func(b *gocqlx.Batch, q *gocqlx.Queryx, playlist PlaylistItem) error {
					return b.BindMap(q, map[string]interface{}{
						"id":      playlist.ID,
						"title":   playlist.Title,
						"album":   playlist.Album,
						"artist":  playlist.Artist,
						"song_id": playlist.SongID,
					})
				},
			},
			{
				name: "Bind",
				methodSong: func(b *gocqlx.Batch, q *gocqlx.Queryx, song Song) error {
					return b.Bind(q, song.ID, song.Title, song.Album, song.Artist, song.Tags, song.Data)
				},
				methodPlaylist: func(b *gocqlx.Batch, q *gocqlx.Queryx, playlist PlaylistItem) error {
					return b.Bind(q, playlist.ID, playlist.Title, playlist.Album, playlist.Artist, playlist.SongID)
				},
			},
			{
				name: "BindStructMap",
				methodSong: func(b *gocqlx.Batch, q *gocqlx.Queryx, song Song) error {
					in := map[string]interface{}{
						"title": song.Title,
						"album": song.Album,
					}
					return b.BindStructMap(q, struct {
						ID     gocql.UUID
						Artist string
						Tags   []string
						Data   []byte
					}{
						ID:     song.ID,
						Artist: song.Artist,
						Tags:   song.Tags,
						Data:   song.Data,
					}, in)
				},
				methodPlaylist: func(b *gocqlx.Batch, q *gocqlx.Queryx, playlist PlaylistItem) error {
					in := map[string]interface{}{
						"title": playlist.Title,
						"album": playlist.Album,
					}
					return b.BindStructMap(q, struct {
						ID     gocql.UUID
						Artist string
						SongID gocql.UUID
					}{
						ID:     playlist.ID,
						Artist: playlist.Artist,
						SongID: playlist.SongID,
					},
						in,
					)
				},
			},
		}
		for _, tcase := range tcases {
			t.Run(tcase.name, func(t *testing.T) {
				insertSong := qb.Insert("batch_test.songs").
					Columns("id", "title", "album", "artist", "tags", "data").Query(session)
				insertPlaylist := qb.Insert("batch_test.playlists").
					Columns("id", "title", "album", "artist", "song_id").Query(session)
				selectSong := qb.Select("batch_test.songs").Where(qb.Eq("id")).Query(session)
				selectPlaylist := qb.Select("batch_test.playlists").Where(qb.Eq("id")).Query(session)
				deleteSong := qb.Delete("batch_test.songs").Where(qb.Eq("id")).Query(session)
				deletePlaylist := qb.Delete("batch_test.playlists").Where(qb.Eq("id")).Query(session)

				b := session.NewBatch(gocql.LoggedBatch)

				if err = tcase.methodSong(b, insertSong, song); err != nil {
					t.Fatal("insert song:", err)
				}
				if err = tcase.methodPlaylist(b, insertPlaylist, playlist); err != nil {
					t.Fatal("insert playList:", err)
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
					t.Fatal("select playList:", err)
				}
				if diff := cmp.Diff(gotPlayList, playlist); diff != "" {
					t.Errorf("expected %v playList, got %v, diff: %q", playlist, gotPlayList, diff)
				}
				if err = deletePlaylist.BindStruct(playlist).Exec(); err != nil {
					t.Error("delete playlist:", err)
				}
				if err = deleteSong.BindStruct(song).Exec(); err != nil {
					t.Error("delete song:", err)
				}
			})
		}
	})
}

func TestBatchAllWrapped(t *testing.T) {
	var (
		gocqlType  = reflect.TypeOf((*gocql.Batch)(nil))
		gocqlxType = reflect.TypeOf((*gocqlx.Batch)(nil))
	)

	for i := 0; i < gocqlType.NumMethod(); i++ {
		m, ok := gocqlxType.MethodByName(gocqlType.Method(i).Name)
		if !ok {
			t.Fatalf("Batch missing method %s", gocqlType.Method(i).Name)
		}

		for j := 0; j < m.Type.NumOut(); j++ {
			if m.Type.Out(j) == gocqlType {
				t.Errorf("Batch method %s not wrapped", m.Name)
			}
		}
	}
}
