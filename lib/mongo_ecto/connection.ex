defmodule Mongo.Ecto.Connection do

  @behaviour Ecto.Adapters.Worker

  alias Mongo.Ecto.Bson

  def connect(opts) do
    opts
    |> Enum.map(fn
      {:hostname, hostname} -> {:host, to_erl(hostname)}
      {:username, username} -> {:login, to_erl(username)}
      {:database, database} -> {:database, to_string(database)}
      {key, value} -> {key, to_erl(value)}
    end)
    |> :mc_worker.start_link
  end

  def disconnect(conn) do
    :mc_worker.disconnect(conn)
  end

  defp to_erl(nil), do: :undefined
  defp to_erl(string) when is_binary(string), do: to_char_list(string)
  defp to_erl(other), do: other

  def all(conn, collection, selector, projector, skip, batch_size) do
    # This is some wired behaviour enforced by the driver, that empty
    # projector should be an empty list, and not empty bson document
    if projector == {} do
      projector = []
    end
    cursor = :mongo.find(conn, collection, selector, projector, skip, batch_size)
    documents = :mc_cursor.rest(cursor)
    :mc_cursor.close(cursor)
    documents
  end

  def delete_all(conn, collection, selector) do
    :mongo.delete(conn, collection, selector)
  end

  def delete(conn, collection, selector) do
    :mongo.delete_one(conn, collection, selector)
  end

  def update_all(conn, collection, selector, command) do
    :mongo.update(conn, collection, selector, command, false, true)
  end

  def update(conn, collection, selector, command) do
    :mongo.update(conn, collection, selector, command, false, false)
  end

  def insert(conn, source, document) do
    :mongo.insert(conn, source, document)
  end

  def command(conn, command) do
    case :mongo.command(conn, command) do
      {true, {:cursor, data}} ->
        {:ok, handle_raw_cursor_data(conn, data)}
      {true, resp} -> {:ok, resp}
      {false, err} -> {:error, err}
    end
  end

  defp handle_raw_cursor_data(conn, data) do
    data = Bson.from_bson(data, nil)

    cursor = :mc_cursor.create(conn, data.ns, data.id, 0, data.firstBatch)
    documents = :mc_cursor.rest(cursor)
    :mc_cursor.close(cursor)

    Enum.map(documents, &Bson.from_bson(&1, nil))
  end
end
