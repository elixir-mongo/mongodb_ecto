defmodule Mongo.Ecto.Connection do
  @moduledoc false

  @behaviour Ecto.Adapters.Worker

  alias Mongo.ReadResult
  alias Mongo.WriteResult

  ## Worker

  def connect(opts) do
    Mongo.Connection.start_link(opts)
  end

  def disconnect(conn) do
    Mongo.Connection.stop(conn)
  end

  ## Callbacks for adapter

  def all(conn, collection, selector, projector, opts \\ []) do
    {:ok, result} = Mongo.find(conn, collection, selector, projector, opts)
    result.docs
  end

  def delete_all(conn, collection, selector) do
    {:ok, %WriteResult{num_removed: removed}} =
      Mongo.remove(conn, collection, selector, multi: true)
    removed
  end

  def delete(conn, collection, selector) do
    Mongo.remove(conn, collection, selector)
  end

  def update_all(conn, collection, selector, command) do
    Mongo.update(conn, collection, selector, command, multi: true)
  end

  def update(conn, collection, selector, command) do
    Mongo.update(conn, collection, selector, command)
  end

  def insert(conn, source, document) do
    Mongo.insert(conn, source, document)
  end

  def command(conn, command) do
    case Mongo.find(conn, "$cmd", command, %{}, num_return: -1) do
      {:ok, %ReadResult{docs: [%{"ok" => 1.0}]}} ->
        :ok
      {:ok, %ReadResult{docs: docs} = res} ->
        {:ok, docs}
      {:error, _} = error ->
        error
    end
  end
end
