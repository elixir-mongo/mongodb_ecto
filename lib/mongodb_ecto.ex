defmodule MongodbEcto do
  @behaviour Ecto.Adapter
  @behaviour Ecto.Adapter.Storage

  defmacro __before_compile__(env) do
    quote do
      def __worker__ do
        unquote(env.module).Worker
      end
    end
  end

  def start_link(repo, opts) do
    opts
    |> prepare_opts(repo)
    |> :mc_worker.start_link
  end

  def stop(repo) do
    :mc_worker.disconnect(repo.__worker__)
  end

  def all(_repo, __query, _params, _opts) do
    {:error, :not_supported}
  end

  def update_all(_repo, _query, _values, _params, _opts) do
    {:error, :not_supported}
  end

  def delete_all(_repo, _query, _params, _opts) do
    {:error, :not_supported}
  end

  def insert(repo, source, params, returning, opts) do
    result =
      :mongo.insert(repo.__worker__, source, to_bson(params))
      |> from_bson

    {:ok, Enum.map(returning, &Map.get(result, &1))}
  end

  def update(_repo, _source, _fields, _filter, _returning, _opts) do
    {:error, :not_supported}
  end

  def delete(_repo, _source, _filter, _opts) do
    {:error, :not_supported}
  end

  @doc """
  Noop for MongoDB, as any databases and collections are created as needed.
  """
  def storage_up(_opts) do
    :ok
  end

  def storage_down(opts) do
    command(opts, {:dropDatabase, 1})
  end

  defp command(opts, command) do
    {:ok, conn} = opts |> prepare_opts |> :mc_worker.start_link
    reply = :mongo.command(conn, command)
    :ok = :mc_worker.disconnect(conn)
    case reply do
      {true, resp} -> {:ok, resp}
      {false, err} -> {:error, err}
    end
  end

  defp prepare_opts(opts) do
    opts
    |> Keyword.take([:database, :r_mode, :w_mode, :timeout,
                     :port, :hostname, :username, :password])
    |> Enum.map(fn
      {:hostname, hostname} -> {:host, to_erl(hostname)}
      {:username, username} -> {:login, to_erl(username)}
      {:database, database} -> {:database, to_string(database)}
      {key, value} when is_binary(value) -> {key, to_erl(value)}
      other -> other
    end)
  end
  defp prepare_opts(opts, repo) do
    opts
    |> prepare_opts
    |> Keyword.put(:register, repo.__worker__)
  end

  defp to_erl(nil), do: :undefined
  defp to_erl(string) when is_binary(string), do: to_char_list(string)
  defp to_erl(other), do: other

  def from_bson(document) do
    document
    |> Tuple.to_list
    |> Enum.chunk(2)
    |> Enum.into(%{}, fn
      [:_id, value] -> {:id, value}
      [key, value]  -> {key, value}
    end)
  end

  def to_bson(document) do
    document
    |> Enum.flat_map(fn {key, value} -> [key, value |> extract_value] end)
    |> List.to_tuple
  end

  def extract_value(%Ecto.Query.Tagged{value: value}), do: value
  def extract_value(value), do: value
end
