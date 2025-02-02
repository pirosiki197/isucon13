package cache

import (
	"context"
	"database/sql"
	"database/sql/driver"
	"fmt"
	"io"
	"log"
	"strings"
	"time"

	"github.com/go-sql-driver/mysql"
	"github.com/motoki317/sc"
	"github.com/traP-jp/isuc/domains"
	"github.com/traP-jp/isuc/normalizer"
)

var queryMap = make(map[string]domains.CachePlanQuery)

var tableSchema = make(map[string]domains.TableSchema)

const cachePlanRaw = `queries:
  - query: SELECT id FROM tags WHERE name = ?;
    type: select
    table: tags
    cache: true
    targets:
      - id
    conditions:
      - column: name
        operator: eq
        placeholder:
          index: 0
  - query: SELECT * FROM livecomment_reports WHERE livestream_id = ?;
    type: select
    table: livecomment_reports
    cache: true
    targets:
      - user_id
      - livestream_id
      - livecomment_id
      - id
      - created_at
    conditions:
      - column: livestream_id
        operator: eq
        placeholder:
          index: 0
  - query: SELECT IFNULL(MAX(tip), 0) FROM livestreams l INNER JOIN livecomments l2 ON l2.livestream_id = l.id WHERE l.id = ?;
    type: select
    table: livestreams
    cache: false
  - query: SELECT COUNT(*) FROM livestream_viewers_history WHERE livestream_id = ?;
    type: select
    table: livestream_viewers_history
    cache: true
    targets:
      - COUNT()
    conditions:
      - column: livestream_id
        operator: eq
        placeholder:
          index: 0
  - query: SELECT COUNT(*) FROM users u INNER JOIN livestreams l ON l.user_id = u.id INNER JOIN reactions r ON r.livestream_id = l.id WHERE u.id = ?;
    type: select
    table: users
    cache: false
  - query: SELECT * FROM tags WHERE id = ?;
    type: select
    table: tags
    cache: true
    targets:
      - name
      - id
    conditions:
      - column: id
        operator: eq
        placeholder:
          index: 0
  - query: SELECT COUNT(*) FROM livestreams l INNER JOIN reactions r ON l.id = r.livestream_id WHERE l.id = ?;
    type: select
    table: livestreams
    cache: false
  - query: SELECT * FROM users;
    type: select
    table: users
    cache: true
    targets:
      - id
      - name
      - display_name
      - password
      - description
  - query: SELECT * FROM livestreams WHERE user_id = ?;
    type: select
    table: livestreams
    cache: true
    targets:
      - title
      - description
      - playlist_url
      - thumbnail_url
      - start_at
      - end_at
      - id
      - user_id
    conditions:
      - column: user_id
        operator: eq
        placeholder:
          index: 0
  - query: SELECT * FROM livestreams;
    type: select
    table: livestreams
    cache: true
    targets:
      - thumbnail_url
      - start_at
      - end_at
      - id
      - user_id
      - title
      - description
      - playlist_url
  - query: SELECT COUNT(*) FROM livestreams l INNER JOIN livecomment_reports r ON r.livestream_id = l.id WHERE l.id = ?;
    type: select
    table: livestreams
    cache: false
  - query: INSERT INTO livecomments (user_id, livestream_id, comment, tip, created_at) VALUES (?);
    type: insert
    table: livecomments
    columns:
      - user_id
      - livestream_id
      - comment
      - tip
      - created_at
  - query: SELECT * FROM tags;
    type: select
    table: tags
    cache: true
    targets:
      - name
      - id
  - query: SELECT * FROM livestream_tags WHERE livestream_id = ?;
    type: select
    table: livestream_tags
    cache: true
    targets:
      - livestream_id
      - tag_id
      - id
    conditions:
      - column: livestream_id
        operator: eq
        placeholder:
          index: 0
  - query: SELECT * FROM livecomments WHERE livestream_id = ? ORDER BY created_at DESC LIMIT ?;
    type: select
    table: livecomments
    cache: true
    targets:
      - tip
      - created_at
      - id
      - user_id
      - livestream_id
      - comment
    conditions:
      - column: livestream_id
        operator: eq
        placeholder:
          index: 0
      - column: LIMIT()
        operator: eq
        placeholder:
          index: 0
          extra: true
    orders:
      - column: created_at
        order: desc
  - query: INSERT INTO ng_words (user_id, livestream_id, word, created_at) VALUES (?);
    type: insert
    table: ng_words
    columns:
      - user_id
      - livestream_id
      - word
      - created_at
  - query: DELETE FROM livestream_viewers_history WHERE user_id = ? AND livestream_id = ?;
    type: delete
    table: livestream_viewers_history
    conditions:
      - column: user_id
        operator: eq
        placeholder:
          index: 0
      - column: livestream_id
        operator: eq
        placeholder:
          index: 1
  - query: SELECT * FROM livestreams ORDER BY id DESC LIMIT ?;
    type: select
    table: livestreams
    cache: true
    targets:
      - description
      - playlist_url
      - thumbnail_url
      - start_at
      - end_at
      - id
      - user_id
      - title
    conditions:
      - column: LIMIT()
        operator: eq
        placeholder:
          index: 0
          extra: true
    orders:
      - column: id
        order: desc
  - query: SELECT * FROM livecomments;
    type: select
    table: livecomments
    cache: true
    targets:
      - livestream_id
      - comment
      - tip
      - created_at
      - id
      - user_id
  - query: SELECT IFNULL(SUM(l2.tip), 0) FROM users u INNER JOIN livestreams l ON l.user_id = u.id INNER JOIN livecomments l2 ON l2.livestream_id = l.id WHERE u.id = ?;
    type: select
    table: users
    cache: false
  - query: SELECT * FROM livestream_tags WHERE tag_id IN (?) ORDER BY livestream_id DESC;
    type: select
    table: livestream_tags
    cache: true
    targets:
      - tag_id
      - id
      - livestream_id
    conditions:
      - column: tag_id
        operator: in
        placeholder:
          index: 0
    orders:
      - column: livestream_id
        order: desc
  - query: DELETE FROM livecomments WHERE id = ? AND livestream_id = ? AND (SELECT COUNT(*) FROM (SELECT ? AS text) AS texts INNER JOIN (SELECT CONCAT('%', ?, '%') AS pattern) AS patterns ON texts.text LIKE patterns.pattern) >= 1;
    type: delete
    table: livecomments
  - query: INSERT INTO livestream_tags (livestream_id, tag_id) VALUES (?);
    type: insert
    table: livestream_tags
    columns:
      - livestream_id
      - tag_id
  - query: SELECT * FROM reactions WHERE livestream_id = ? ORDER BY created_at DESC;
    type: select
    table: reactions
    cache: true
    targets:
      - id
      - user_id
      - livestream_id
      - emoji_name
      - created_at
    conditions:
      - column: livestream_id
        operator: eq
        placeholder:
          index: 0
    orders:
      - column: created_at
        order: desc
  - query: SELECT * FROM users WHERE id = ?;
    type: select
    table: users
    cache: true
    targets:
      - password
      - description
      - id
      - name
      - display_name
    conditions:
      - column: id
        operator: eq
        placeholder:
          index: 0
  - query: SELECT image FROM icons WHERE user_id = ?;
    type: select
    table: icons
    cache: true
    targets:
      - image
    conditions:
      - column: user_id
        operator: eq
        placeholder:
          index: 0
  - query: INSERT INTO users (name, display_name, description, password) VALUES (?);
    type: insert
    table: users
    columns:
      - name
      - display_name
      - description
      - password
  - query: SELECT IFNULL(SUM(tip), 0) FROM livecomments;
    type: select
    table: livecomments
    cache: false
  - query: INSERT INTO livestreams (user_id, title, description, playlist_url, thumbnail_url, start_at, end_at) VALUES (?);
    type: insert
    table: livestreams
    columns:
      - user_id
      - title
      - description
      - playlist_url
      - thumbnail_url
      - start_at
      - end_at
  - query: INSERT INTO livestream_viewers_history (user_id, livestream_id, created_at) VALUES (?);
    type: insert
    table: livestream_viewers_history
    columns:
      - user_id
      - livestream_id
      - created_at
  - query: INSERT INTO reactions (user_id, livestream_id, emoji_name, created_at) VALUES (?);
    type: insert
    table: reactions
    columns:
      - user_id
      - livestream_id
      - emoji_name
      - created_at
  - query: SELECT * FROM livecomments WHERE id = ?;
    type: select
    table: livecomments
    cache: true
    targets:
      - id
      - user_id
      - livestream_id
      - comment
      - tip
      - created_at
    conditions:
      - column: id
        operator: eq
        placeholder:
          index: 0
  - query: DELETE FROM icons WHERE user_id = ?;
    type: delete
    table: icons
    conditions:
      - column: user_id
        operator: eq
        placeholder:
          index: 0
  - query: SELECT * FROM livecomments WHERE livestream_id = ? ORDER BY created_at DESC LIMIT ?;
    type: select
    table: livecomments
    cache: true
    targets:
      - id
      - user_id
      - livestream_id
      - comment
      - tip
      - created_at
    conditions:
      - column: livestream_id
        operator: eq
        placeholder:
          index: 0
      - column: LIMIT()
        operator: eq
        placeholder:
          index: 0
          extra: true
    orders:
      - column: created_at
        order: desc
  - query: SELECT slot FROM reservation_slots WHERE start_at = ? AND end_at = ?;
    type: select
    table: reservation_slots
    cache: true
    targets:
      - slot
    conditions:
      - column: start_at
        operator: eq
        placeholder:
          index: 0
      - column: end_at
        operator: eq
        placeholder:
          index: 1
  - query: SELECT * FROM reservation_slots WHERE start_at >= ? AND end_at <= ? FOR UPDATE;
    type: select
    table: reservation_slots
    cache: false
  - query: SELECT * FROM reactions WHERE livestream_id = ? ORDER BY created_at DESC LIMIT ?;
    type: select
    table: reactions
    cache: true
    targets:
      - user_id
      - livestream_id
      - emoji_name
      - created_at
      - id
    conditions:
      - column: livestream_id
        operator: eq
        placeholder:
          index: 0
      - column: LIMIT()
        operator: eq
        placeholder:
          index: 0
          extra: true
    orders:
      - column: created_at
        order: desc
  - query: SELECT * FROM users WHERE name = ?;
    type: select
    table: users
    cache: true
    targets:
      - name
      - display_name
      - password
      - description
      - id
    conditions:
      - column: name
        operator: eq
        placeholder:
          index: 0
  - query: SELECT * FROM livestreams WHERE id = ?;
    type: select
    table: livestreams
    cache: true
    targets:
      - user_id
      - title
      - description
      - playlist_url
      - thumbnail_url
      - start_at
      - end_at
      - id
    conditions:
      - column: id
        operator: eq
        placeholder:
          index: 0
  - query: SELECT IFNULL(SUM(l2.tip), 0) FROM livestreams l INNER JOIN livecomments l2 ON l.id = l2.livestream_id WHERE l.id = ?;
    type: select
    table: livestreams
    cache: false
  - query: SELECT id, user_id, livestream_id, word FROM ng_words WHERE user_id = ? AND livestream_id = ?;
    type: select
    table: ng_words
    cache: true
    targets:
      - id
      - user_id
      - livestream_id
      - word
    conditions:
      - column: user_id
        operator: eq
        placeholder:
          index: 0
      - column: livestream_id
        operator: eq
        placeholder:
          index: 1
  - query: SELECT * FROM livecomments WHERE livestream_id = ? ORDER BY created_at DESC;
    type: select
    table: livecomments
    cache: true
    targets:
      - comment
      - tip
      - created_at
      - id
      - user_id
      - livestream_id
    conditions:
      - column: livestream_id
        operator: eq
        placeholder:
          index: 0
    orders:
      - column: created_at
        order: desc
  - query: UPDATE reservation_slots FORCE INDEX (` + "`" + `start_at_end_at` + "`" + `) SET slot = slot - 1 WHERE start_at >= ? AND end_at <= ?;
    type: update
    table: reservation_slots
    targets:
      - column: slot
        placeholder:
          index: 0
  - query: SELECT COUNT(*) FROM users u INNER JOIN livestreams l ON l.user_id = u.id INNER JOIN reactions r ON r.livestream_id = l.id WHERE u.name = ?;
    type: select
    table: users
    cache: false
  - query: SELECT * FROM ng_words WHERE user_id = ? AND livestream_id = ? ORDER BY created_at DESC;
    type: select
    table: ng_words
    cache: true
    targets:
      - word
      - created_at
      - id
      - user_id
      - livestream_id
    conditions:
      - column: user_id
        operator: eq
        placeholder:
          index: 0
      - column: livestream_id
        operator: eq
        placeholder:
          index: 1
    orders:
      - column: created_at
        order: desc
  - query: INSERT INTO livecomment_reports (user_id, livestream_id, livecomment_id, created_at) VALUES (?);
    type: insert
    table: livecomment_reports
    columns:
      - user_id
      - livestream_id
      - livecomment_id
      - created_at
  - query: SELECT * FROM livecomments WHERE livestream_id = ?;
    type: select
    table: livecomments
    cache: true
    targets:
      - comment
      - tip
      - created_at
      - id
      - user_id
      - livestream_id
    conditions:
      - column: livestream_id
        operator: eq
        placeholder:
          index: 0
  - query: SELECT r.emoji_name FROM users u INNER JOIN livestreams l ON l.user_id = u.id INNER JOIN reactions r ON r.livestream_id = l.id WHERE u.name = ? GROUP BY emoji_name ORDER BY COUNT(*) DESC, emoji_name DESC LIMIT 1;
    type: select
    table: users
    cache: false
  - query: SELECT * FROM livestreams WHERE id = ? AND user_id = ?;
    type: select
    table: livestreams
    cache: true
    targets:
      - end_at
      - id
      - user_id
      - title
      - description
      - playlist_url
      - thumbnail_url
      - start_at
    conditions:
      - column: id
        operator: eq
        placeholder:
          index: 0
      - column: user_id
        operator: eq
        placeholder:
          index: 1
  - query: SELECT * FROM themes WHERE user_id = ?;
    type: select
    table: themes
    cache: true
    targets:
      - user_id
      - dark_mode
      - id
    conditions:
      - column: user_id
        operator: eq
        placeholder:
          index: 0
  - query: SELECT id FROM users WHERE name = ?;
    type: select
    table: users
    cache: true
    targets:
      - id
    conditions:
      - column: name
        operator: eq
        placeholder:
          index: 0
  - query: INSERT INTO themes (user_id, dark_mode) VALUES (?);
    type: insert
    table: themes
    columns:
      - user_id
      - dark_mode
  - query: SELECT COUNT(*) FROM livestreams l INNER JOIN reactions r ON r.livestream_id = l.id WHERE l.id = ?;
    type: select
    table: livestreams
    cache: false
  - query: INSERT INTO icons (user_id, image) VALUES (?);
    type: insert
    table: icons
    columns:
      - user_id
      - image
  - query: SELECT * FROM ng_words WHERE livestream_id = ?;
    type: select
    table: ng_words
    cache: true
    targets:
      - user_id
      - livestream_id
      - word
      - created_at
      - id
    conditions:
      - column: livestream_id
        operator: eq
        placeholder:
          index: 0
  - query: SELECT COUNT(*) FROM livestreams l INNER JOIN livestream_viewers_history h ON h.livestream_id = l.id WHERE l.id = ?;
    type: select
    table: livestreams
    cache: false
`

const schemaRaw = `USE ` + "`" + `isupipe` + "`" + `;

DROP TABLE IF EXISTS ` + "`" + `users` + "`" + `;
DROP TABLE IF EXISTS ` + "`" + `icons` + "`" + `;
DROP TABLE IF EXISTS ` + "`" + `themes` + "`" + `;
DROP TABLE IF EXISTS ` + "`" + `livestreams` + "`" + `;
DROP TABLE IF EXISTS ` + "`" + `reservation_slots` + "`" + `;
DROP TABLE IF EXISTS ` + "`" + `tags` + "`" + `;
DROP TABLE IF EXISTS ` + "`" + `livestream_tags` + "`" + `;
DROP TABLE IF EXISTS ` + "`" + `livestream_viewers_history` + "`" + `;
DROP TABLE IF EXISTS ` + "`" + `livecomments` + "`" + `;
DROP TABLE IF EXISTS ` + "`" + `livecomment_reports` + "`" + `;
DROP TABLE IF EXISTS ` + "`" + `ng_words` + "`" + `;
DROP TABLE IF EXISTS ` + "`" + `reactions` + "`" + `;

-- ユーザ (配信者、視聴者)
CREATE TABLE ` + "`" + `users` + "`" + ` (
  ` + "`" + `id` + "`" + ` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ` + "`" + `name` + "`" + ` VARCHAR(255) NOT NULL,
  ` + "`" + `display_name` + "`" + ` VARCHAR(255) NOT NULL,
  ` + "`" + `password` + "`" + ` VARCHAR(255) NOT NULL,
  ` + "`" + `description` + "`" + ` TEXT NOT NULL,
  UNIQUE ` + "`" + `uniq_user_name` + "`" + ` (` + "`" + `name` + "`" + `)
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- プロフィール画像
CREATE TABLE ` + "`" + `icons` + "`" + ` (
  ` + "`" + `id` + "`" + ` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ` + "`" + `user_id` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `image` + "`" + ` LONGBLOB NOT NULL,
  INDEX ` + "`" + `idx_user_id` + "`" + ` (` + "`" + `user_id` + "`" + `)
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- ユーザごとのカスタムテーマ
CREATE TABLE ` + "`" + `themes` + "`" + ` (
  ` + "`" + `id` + "`" + ` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ` + "`" + `user_id` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `dark_mode` + "`" + ` BOOLEAN NOT NULL,
  INDEX ` + "`" + `idx_user_id` + "`" + ` (` + "`" + `user_id` + "`" + `)
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- ライブ配信
CREATE TABLE ` + "`" + `livestreams` + "`" + ` (
  ` + "`" + `id` + "`" + ` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ` + "`" + `user_id` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `title` + "`" + ` VARCHAR(255) NOT NULL,
  ` + "`" + `description` + "`" + ` text NOT NULL,
  ` + "`" + `playlist_url` + "`" + ` VARCHAR(255) NOT NULL,
  ` + "`" + `thumbnail_url` + "`" + ` VARCHAR(255) NOT NULL,
  ` + "`" + `start_at` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `end_at` + "`" + ` BIGINT NOT NULL,
  INDEX ` + "`" + `idx_user_id` + "`" + ` (` + "`" + `user_id` + "`" + `)
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- ライブ配信予約枠
CREATE TABLE ` + "`" + `reservation_slots` + "`" + ` (
  ` + "`" + `id` + "`" + ` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ` + "`" + `slot` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `start_at` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `end_at` + "`" + ` BIGINT NOT NULL,
  INDEX ` + "`" + `start_at_end_at` + "`" + ` (` + "`" + `start_at` + "`" + `, ` + "`" + `end_at` + "`" + `)
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- ライブストリームに付与される、サービスで定義されたタグ
CREATE TABLE ` + "`" + `tags` + "`" + ` (
  ` + "`" + `id` + "`" + ` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ` + "`" + `name` + "`" + ` VARCHAR(255) NOT NULL,
  UNIQUE ` + "`" + `uniq_tag_name` + "`" + ` (` + "`" + `name` + "`" + `)
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- ライブ配信とタグの中間テーブル
CREATE TABLE ` + "`" + `livestream_tags` + "`" + ` (
  ` + "`" + `id` + "`" + ` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ` + "`" + `livestream_id` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `tag_id` + "`" + ` BIGINT NOT NULL,
  INDEX ` + "`" + `idx_livestream_id` + "`" + ` (` + "`" + `livestream_id` + "`" + `),
  INDEX ` + "`" + `idx_tag_id_livestream_id` + "`" + ` (` + "`" + `tag_id` + "`" + `, ` + "`" + `livestream_id` + "`" + `)
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- ライブ配信視聴履歴
CREATE TABLE ` + "`" + `livestream_viewers_history` + "`" + ` (
  ` + "`" + `id` + "`" + ` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ` + "`" + `user_id` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `livestream_id` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `created_at` + "`" + ` BIGINT NOT NULL
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- ライブ配信に対するライブコメント
CREATE TABLE ` + "`" + `livecomments` + "`" + ` (
  ` + "`" + `id` + "`" + ` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ` + "`" + `user_id` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `livestream_id` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `comment` + "`" + ` VARCHAR(255) NOT NULL,
  ` + "`" + `tip` + "`" + ` BIGINT NOT NULL DEFAULT 0,
  ` + "`" + `created_at` + "`" + ` BIGINT NOT NULL,
  INDEX ` + "`" + `idx_livestream_id` + "`" + ` (` + "`" + `livestream_id` + "`" + `)
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- ユーザからのライブコメントのスパム報告
CREATE TABLE ` + "`" + `livecomment_reports` + "`" + ` (
  ` + "`" + `id` + "`" + ` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ` + "`" + `user_id` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `livestream_id` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `livecomment_id` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `created_at` + "`" + ` BIGINT NOT NULL
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;

-- 配信者からのNGワード登録
CREATE TABLE ` + "`" + `ng_words` + "`" + ` (
  ` + "`" + `id` + "`" + ` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ` + "`" + `user_id` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `livestream_id` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `word` + "`" + ` VARCHAR(255) NOT NULL,
  ` + "`" + `created_at` + "`" + ` BIGINT NOT NULL,
  INDEX ` + "`" + `idx_livestream_id` + "`" + ` (` + "`" + `livestream_id` + "`" + `),
  INDEX ` + "`" + `idx_user_id_livestream_id` + "`" + ` (` + "`" + `user_id` + "`" + `, ` + "`" + `livestream_id` + "`" + `)
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
CREATE INDEX ng_words_word ON ng_words(` + "`" + `word` + "`" + `);

-- ライブ配信に対するリアクション
CREATE TABLE ` + "`" + `reactions` + "`" + ` (
  ` + "`" + `id` + "`" + ` BIGINT NOT NULL AUTO_INCREMENT PRIMARY KEY,
  ` + "`" + `user_id` + "`" + ` BIGINT NOT NULL,
  ` + "`" + `livestream_id` + "`" + ` BIGINT NOT NULL,
  -- :innocent:, :tada:, etc...
  ` + "`" + `emoji_name` + "`" + ` VARCHAR(255) NOT NULL,
  ` + "`" + `created_at` + "`" + ` BIGINT NOT NULL,
  INDEX ` + "`" + `idx_livestream_id` + "`" + ` (` + "`" + `livestream_id` + "`" + `)
) ENGINE=InnoDB CHARACTER SET utf8mb4 COLLATE utf8mb4_bin;
`

func init() {
	sql.Register("mysql+cache", CacheDriver{})

	schema, err := domains.LoadTableSchema(schemaRaw)
	if err != nil {
		panic(err)
	}
	for _, table := range schema {
		tableSchema[table.TableName] = table
	}

	plan, err := domains.LoadCachePlan(strings.NewReader(cachePlanRaw))
	if err != nil {
		panic(err)
	}

	for _, query := range plan.Queries {
		normalized := normalizer.NormalizeQuery(query.Query)
		query.Query = normalized // make sure to use normalized query
		queryMap[normalized] = *query
		if query.Type != domains.CachePlanQueryType_SELECT || !query.Select.Cache {
			continue
		}

		conditions := query.Select.Conditions
		if isSingleUniqueCondition(conditions, query.Select.Table) {
			caches[normalized] = &cacheWithInfo{
				Cache:      sc.NewMust(replaceFn, 10*time.Minute, 10*time.Minute),
				query:      normalized,
				info:       *query.Select,
				uniqueOnly: true,
			}
			continue
		}
		caches[query.Query] = &cacheWithInfo{
			Cache:      sc.NewMust(replaceFn, 10*time.Minute, 10*time.Minute),
			query:      query.Query,
			info:       *query.Select,
			uniqueOnly: false,
		}

		// TODO: if query is like "SELECT * FROM WHERE pk IN (?, ?, ...)", generate cache with query "SELECT * FROM table WHERE pk = ?"
	}

	for _, cache := range caches {
		cacheByTable[cache.info.Table] = append(cacheByTable[cache.info.Table], cache)
	}
}

var _ driver.Driver = CacheDriver{}

type CacheDriver struct{}

func (d CacheDriver) Open(dsn string) (driver.Conn, error) {
	cfg, err := mysql.ParseDSN(dsn)
	if err != nil {
		return nil, err
	}
	c, err := mysql.NewConnector(cfg)
	if err != nil {
		return nil, err
	}
	conn, err := c.Connect(context.Background())
	if err != nil {
		return nil, err
	}
	return &cacheConn{inner: conn}, nil
}

var (
	_ driver.Conn           = &cacheConn{}
	_ driver.ConnBeginTx    = &cacheConn{}
	_ driver.Pinger         = &cacheConn{}
	_ driver.QueryerContext = &cacheConn{}
	_ driver.ExecerContext  = &cacheConn{}
)

type cacheConn struct {
	inner   driver.Conn
	tx      bool
	txStart int64 // time.Time.UnixNano()
	cleanUp cleanUpTask
}

func (c *cacheConn) Prepare(rawQuery string) (driver.Stmt, error) {
	normalizedQuery := normalizer.NormalizeQuery(rawQuery)

	queryInfo, ok := queryMap[normalizedQuery]
	if !ok {
		// unknown (insert, update, delete) query
		if !strings.HasPrefix(strings.ToUpper(normalizedQuery), "SELECT") {
			log.Println("unknown query:", normalizedQuery)
			PurgeAllCaches()
		}
		return c.inner.Prepare(rawQuery)
	}

	if queryInfo.Type == domains.CachePlanQueryType_SELECT && !queryInfo.Select.Cache {
		return c.inner.Prepare(rawQuery)
	}

	innerStmt, err := c.inner.Prepare(rawQuery)
	if err != nil {
		return nil, err
	}
	return &customCacheStatement{
		inner:     innerStmt,
		conn:      c,
		rawQuery:  rawQuery,
		query:     normalizedQuery,
		queryInfo: queryInfo,
	}, nil
}

func (c *cacheConn) Close() error {
	return c.inner.Close()
}

func (c *cacheConn) Begin() (driver.Tx, error) {
	inner, err := c.BeginTx(context.Background(), driver.TxOptions{})
	if err != nil {
		return nil, err
	}
	c.tx = true
	c.txStart = time.Now().UnixNano()
	return &cacheTx{conn: c, inner: inner}, nil
}

func (c *cacheConn) BeginTx(ctx context.Context, opts driver.TxOptions) (driver.Tx, error) {
	var inner driver.Tx
	var err error
	if i, ok := c.inner.(driver.ConnBeginTx); ok {
		inner, err = i.BeginTx(ctx, opts)
		if err != nil {
			return nil, err
		}
	} else {
		inner, err = c.inner.Begin()
		if err != nil {
			return nil, err
		}
	}
	c.tx = true
	c.txStart = time.Now().UnixNano()
	return &cacheTx{conn: c, inner: inner}, nil
}

func (c *cacheConn) Ping(ctx context.Context) error {
	if i, ok := c.inner.(driver.Pinger); ok {
		return i.Ping(ctx)
	}
	return nil
}

var _ driver.Tx = &cacheTx{}

type cacheTx struct {
	conn  *cacheConn
	inner driver.Tx
}

func (t *cacheTx) Commit() error {
	t.conn.tx = false
	defer func() {
		for _, c := range t.conn.cleanUp.purge {
			c.Purge()
		}
		for _, forget := range t.conn.cleanUp.forget {
			forget.cache.Forget(forget.key)
		}
		t.conn.cleanUp.reset()
	}()
	return t.inner.Commit()
}

func (t *cacheTx) Rollback() error {
	t.conn.tx = false
	// no need to clean up
	t.conn.cleanUp.reset()
	return t.inner.Rollback()
}

var _ driver.Rows = &cacheRows{}

type cacheRows struct {
	cached  bool
	columns []string
	rows    sliceRows
}

func newCacheRows(inner driver.Rows) (*cacheRows, error) {
	r := new(cacheRows)

	err := r.cacheInnerRows(inner)
	if err != nil {
		return nil, err
	}

	return r, nil
}

func (r *cacheRows) clone() *cacheRows {
	if !r.cached {
		panic("cannot clone uncached rows")
	}
	return &cacheRows{
		cached:  r.cached,
		columns: r.columns,
		rows:    r.rows.clone(),
	}
}

func (r *cacheRows) Columns() []string {
	if !r.cached {
		panic("cannot get columns of uncached rows")
	}
	return r.columns
}

func (r *cacheRows) Close() error {
	r.rows.reset()
	return nil
}

func (r *cacheRows) Next(dest []driver.Value) error {
	if !r.cached {
		return fmt.Errorf("cannot get next row of uncached rows")
	}
	return r.rows.next(dest)
}

func mergeCachedRows(rows []*cacheRows) *cacheRows {
	if len(rows) == 0 {
		return nil
	}
	if len(rows) == 1 {
		return rows[0]
	}

	mergedSlice := sliceRows{}
	for _, r := range rows {
		mergedSlice.concat(r.rows)
	}

	return &cacheRows{
		cached:  true,
		columns: rows[0].columns,
		rows:    mergedSlice,
	}
}

func (r *cacheRows) cacheInnerRows(inner driver.Rows) error {
	columns := inner.Columns()
	r.columns = columns
	dest := make([]driver.Value, len(columns))

	for {
		err := inner.Next(dest)
		if err == io.EOF {
			break
		} else if err != nil {
			return err
		}

		cachedRow := make(row, len(dest))
		for i := 0; i < len(dest); i++ {
			switch v := dest[i].(type) {
			case int64, uint64, float64, string, bool, time.Time, nil: // no need to copy
				cachedRow[i] = v
			case []byte: // copy to prevent mutation
				data := make([]byte, len(v))
				copy(data, v)
				cachedRow[i] = data
			default:
				// TODO: handle other types
				// Should we mark this row as uncacheable?
			}
		}
		r.rows.append(cachedRow)
	}

	r.cached = true

	return nil
}

type row = []driver.Value

type sliceRows struct {
	rows []row
	idx  int
}

func (r sliceRows) clone() sliceRows {
	rows := make([]row, len(r.rows))
	copy(rows, r.rows)
	return sliceRows{rows: rows}
}

func (r *sliceRows) append(row ...row) {
	r.rows = append(r.rows, row...)
}

func (r *sliceRows) concat(rows sliceRows) {
	r.rows = append(r.rows, rows.rows...)
}

func (r *sliceRows) reset() {
	r.idx = 0
}

func (r *sliceRows) next(dest []driver.Value) error {
	if r.idx >= len(r.rows) {
		r.reset()
		return io.EOF
	}
	row := r.rows[r.idx]
	r.idx++
	copy(dest, row)
	return nil
}
