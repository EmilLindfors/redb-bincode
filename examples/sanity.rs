use redb_bincode::*;

fn main() -> anyhow::Result<()> {
    let db = redb::Database::create("test_db")?;

    let db = Database::from(db);

    #[derive(bincode::Encode, bincode::Decode, Debug)]
    struct Something {
        foo: u64,
        bar: String,
    }
    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table::<String, Something>("test_table")?;
        table.insert(
            "foo",
            &Something {
                foo: 13u64,
                bar: "bar".into(),
            },
        )?;
    }
    write_txn.commit()?;

    let read_txn = db.begin_read()?;

    {
        let table = read_txn.open_table::<String, Something>("test_table")?;
        let v = table.get("foo")?.expect("some");
        println!("{:?}", v.value());
    }

    let write_txn = db.begin_write()?;
    {
        let mut table = write_txn.open_table::<String, Something>("test_table")?;
        let prev = table.remove("foo")?.map(|v| v.value()).transpose()?;
        println!("prev: {:?}", prev);
        let v = table.get("foo")?.map(|v| v.value()).transpose()?;
        println!("now: {:?}", v);
    }
    write_txn.commit()?;
    Ok(())
}
