/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *   http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 *
 */
package postgresql

import (
	"context"
	"database/sql"
	"errors"
	"fmt"
	"sync"
	"sync/atomic"
	"time"

	"github.com/apache/kvrocks-controller/consts"
	"github.com/apache/kvrocks-controller/store/engine"
	"github.com/lib/pq"
)

const (
	lockTTL                      = 6 * time.Second
	listenerMinReconnectInterval = 10 * time.Second
	listenerMaxReconnectInterval = 1 * time.Minute
)

const defaultElectPath = "/kvrocks/controller/leader"
const dataTableName = "kv"
const lockTableName = "lock"

type Config struct {
	Addrs         []string `yaml:"addrs"`
	Username      string   `yaml:"username"`
	Password      string   `yaml:"password"`
	DBName        string   `yaml:"db_name"`
	NotifyChannel string   `yaml:"notify_channel"`
	ElectPath     string   `yaml:"elect_path"`
}

type Postgresql struct {
	db            *sql.DB
	listener      *pq.Listener
	dataTableName string
	lockTableName string

	leaderMu  sync.Mutex
	leaderID  string
	myId      string
	electPath string
	isReady   atomic.Bool

	quitCh         chan struct{}
	wg             sync.WaitGroup
	lockChangeCh   chan bool
	leaderChangeCh chan bool
}

func New(id string, cfg *Config) (*Postgresql, error) {
	if len(id) == 0 {
		return nil, errors.New("id must NOT be a empty string")
	}

	connStr := fmt.Sprintf("postgres://%s:%s@%s/%s?sslmode=disable", cfg.Username, cfg.Password, cfg.Addrs[0], cfg.DBName)
	db, err := sql.Open("postgres", connStr)
	if err != nil {
		return nil, err
	}

	listener := pq.NewListener(connStr, listenerMinReconnectInterval, listenerMaxReconnectInterval, nil)
	err = listener.Listen(cfg.NotifyChannel)
	if err != nil {
		return nil, err
	}

	electPath := defaultElectPath
	if cfg.ElectPath != "" {
		electPath = defaultElectPath
	}

	p := &Postgresql{
		myId:           id,
		electPath:      electPath,
		db:             db,
		listener:       listener,
		quitCh:         make(chan struct{}),
		lockChangeCh:   make(chan bool),
		leaderChangeCh: make(chan bool),
	}
	p.isReady.Store(false)
	p.wg.Add(2)
	go p.electLoop()
	go p.observeLeaderEvent()
	return p, nil
}

func (p *Postgresql) ID() string {
	return p.myId
}

func (p *Postgresql) Leader() string {
	p.leaderMu.Lock()
	defer p.leaderMu.Unlock()
	return p.leaderID
}

func (p *Postgresql) LeaderChange() <-chan bool {
	return p.leaderChangeCh
}

func (p *Postgresql) IsReady(ctx context.Context) bool {
	for {
		select {
		case <-p.quitCh:
			return false
		case <-time.After(100 * time.Millisecond):
			if p.isReady.Load() {
				return true
			}
		case <-ctx.Done():
			return p.isReady.Load()
		}
	}
}

func (p *Postgresql) Get(ctx context.Context, key string) ([]byte, error) {
	var value []byte
	query := fmt.Sprintf("SELECT value FROM %s WHERE key = $1", dataTableName)

	row := p.db.QueryRow(query, key)
	err := row.Scan(&value)
	if err == sql.ErrNoRows {
		return nil, consts.ErrNotFound
	}
	if err != nil {
		return nil, err
	}
	return value, nil
}

func (p *Postgresql) Exists(ctx context.Context, key string) (bool, error) {
	_, err := p.Get(ctx, key)
	if err != nil {
		if errors.Is(err, consts.ErrNotFound) {
			return false, nil
		}
		return false, err
	}
	return true, nil
}

func (p *Postgresql) Set(ctx context.Context, key string, value []byte) error {

}

func (p *Postgresql) Delete(ctx context.Context, key string) error {

}

func (p *Postgresql) List(ctx context.Context, prefix string) ([]engine.Entry, error) {

}

func (p *Postgresql) electLoop() {

}

func (p *Postgresql) observeLeaderEvent() {

}

func (p *Postgresql) Close() error {
	close(p.quitCh)
	p.wg.Wait()
	p.listener.Close()
	return p.db.Close()
}
