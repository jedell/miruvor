defmodule Miruvor.RocksdbTest do
  use ExUnit.Case

  alias Miruvor.RocksDB

  test "create table" do
    handle = RocksDB.create_table()
    assert :ok = :rocksdb.close(handle)
  end

  test "create table with name" do
    handle = RocksDB.create_table("test")
    assert :ok = :rocksdb.close(handle)
  end

  test "create table with name and path" do
    handle = RocksDB.create_table("test", "test")
    assert :ok = :rocksdb.close(handle)
  end

  test "put key and value in db" do
    handle = RocksDB.create_table()
    assert {:ok, "value"} = RocksDB.put(handle, "key", "value")
    assert :ok = :rocksdb.close(handle)
  end

  test "get value of key" do
    handle = RocksDB.create_table()
    assert {:ok, "value"} = RocksDB.put(handle, "key", "value")
    assert {:ok, "value"} = RocksDB.get(handle, "key")
    assert :ok = :rocksdb.close(handle)
  end

end
