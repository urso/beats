package statestore

import "testing"

func TestTxFailsAfterClose(t *testing.T) {
	cases := map[string](func(*Tx) error){
		"Rollback": (*Tx).Rollback,
		"Commit":   (*Tx).Commit,
		"Lookup": func(tx *Tx) error {
			_, err := tx.Has("test")
			return err
		},
		"Read": func(tx *Tx) error {
			_, err := tx.Get("test")
			return err
		},
		"Insert": func(tx *Tx) error {
			_, err := tx.Insert("test")
			return err
		},
		"Remove": func(tx *Tx) error { return tx.Remove("test") },
		"Set":    func(tx *Tx) error { return tx.Set("test", "value") },
		"Update": func(tx *Tx) error { return tx.Update("test", "value") },
		"EachKey": func(tx *Tx) error {
			return tx.EachKey(func(_ Key) (bool, error) { return true, nil })
		},
		"Each": func(tx *Tx) error {
			return tx.Each(func(_ Key, _ ValueDecoder) (bool, error) { return true, nil })
		},
	}

	for test, op := range cases {
		t.Run(test, func(t *testing.T) {
			backend := newMockTx()
			backend.OnRollback().Once().Return(nil)
			backend.OnClose().Once().Return(nil)
			tx := newTx(backend, true)
			tx.Close()

			err := op(tx)
			if err == nil {
				t.Fatal("Expected operation to fail")
			}
			if err != errTxClosed {
				t.Fatalf("Expected standard close error, but got: %v", err)
			}
		})
	}

}

func TestTxNoUpdateOpsOnReadonlyAllowed(t *testing.T) {
	cases := map[string](func(*Tx) error){
		"Rollback": (*Tx).Rollback,
		"Commit":   (*Tx).Commit,
		"Insert": func(tx *Tx) error {
			_, err := tx.Insert("test")
			return err
		},
		"Remove": func(tx *Tx) error { return tx.Remove("test") },
		"Set":    func(tx *Tx) error { return tx.Set("test", "value") },
		"Update": func(tx *Tx) error { return tx.Update("test", "value") },
	}

	for test, op := range cases {
		t.Run(test, func(t *testing.T) {
			backend := newMockTx()

			tx := newTx(backend, true)

			err := op(tx)
			if err == nil {
				t.Fatal("Expected operation to fail")
			}
		})
	}
}
