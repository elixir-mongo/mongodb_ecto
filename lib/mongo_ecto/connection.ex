defmodule Mongo.Ecto.Connection do
  @moduledoc false

  require Logger

  alias Mongo.Ecto.NormalizedQuery.{
    AggregateQuery,
    CommandQuery,
    CountQuery,
    ReadQuery,
    WriteQuery
  }

  alias Mongo.Query

  def child_spec(opts) do
    # Rename the `:mongo_url` key so that the driver can parse it
    opts =
      Enum.map(opts, fn
        {:mongo_url, value} -> {:url, value}
        {key, value} -> {key, value}
      end)

    Mongo.child_spec(opts)
  end

  ## Worker

  def init(_config) do
  end

  def storage_down(opts) do
    {:ok, _apps} = Application.ensure_all_started(:mongodb)
    {:ok, conn} = Mongo.start_link(opts)

    try do
      Mongo.command!(conn, dropDatabase: 1)
      :ok
    after
      GenServer.stop(conn)
    end
  end

  def storage_status(opts) do
    {:ok, _apps} = Application.ensure_all_started(:mongodb)
    {:ok, conn} = Mongo.start_link(opts)

    case Mongo.command(conn, %{ping: true}) do
      {:ok, %{"ok" => 1.0}} -> :up
      _ -> :down
    end
  end

  ## Callbacks for adapter

  def read(repo, query, opts \\ [])

  def read(repo, %ReadQuery{} = query, opts) do
    projection = Map.put_new(query.projection, :_id, false)
    opts = [projection: projection, sort: query.order] ++ query.opts ++ opts
    coll = query.coll
    query = query.query

    query(repo, :find, [coll, query], opts)
  end

  def read(repo, %CountQuery{} = query, opts) do
    coll = query.coll
    opts = query.opts ++ opts
    query = query.query

    [%{"value" => query(repo, :count!, [coll, query], opts)}]
  end

  def read(repo, %AggregateQuery{} = query, opts) do
    coll = query.coll
    opts = query.opts ++ opts
    pipeline = query.pipeline

    query(repo, :aggregate, [coll, pipeline], opts)
  end

  def delete_all(repo, %WriteQuery{} = query, opts) do
    coll = query.coll
    opts = query.opts ++ opts
    query = query.query

    %{deleted_count: n} = query(repo, :delete_many!, [coll, query], opts)

    n
  end

  def delete(repo, %WriteQuery{} = query, opts) do
    coll = query.coll
    opts = query.opts ++ opts
    query = query.query

    case query(repo, :delete_one, [coll, query], opts) do
      {:ok, %{deleted_count: 1}} ->
        {:ok, []}

      {:ok, _} ->
        {:error, :stale}

      {:error, error} ->
        check_constraint_errors(error)
    end
  end

  def update_all(repo, %WriteQuery{} = query, opts) do
    coll = query.coll
    command = query.command
    opts = query.opts ++ opts
    query = query.query

    case query(repo, :update_many, [coll, query, command], opts) do
      {:ok, %Mongo.UpdateResult{modified_count: m} = _result} ->
        m

      {:error, error} ->
        check_constraint_errors(error)
    end
  end

  def update_one(repo, %WriteQuery{} = query, opts) do
    coll = query.coll
    command = query.command
    opts = query.opts ++ opts
    query = query.query

    case query(repo, :update_one, [coll, query, command], opts) do
      {:ok, %{modified_count: 1}} ->
        {:ok, []}

      # TODO - maybe?!  What constitutes a successful upsert.
      # {:ok, %{upserted_ids: [_|_]}} ->
      #   {:ok, []}

      {:ok, _} ->
        {:error, :stale}

      {:error, error} ->
        check_constraint_errors(error)
    end
  end

  def find_one_and_update(repo, %WriteQuery{} = query, opts) do
    coll = query.coll
    command = query.command
    returning = query.returning
    opts = query.opts ++ opts
    query = query.query

    case query(repo, :find_one_and_update, [coll, query, command], opts) |> IO.inspect(label: "find_one_and_update res`") do
      {:ok,
       %Mongo.FindAndModifyResult{matched_count: 0, updated_existing: false, upserted_id: nil}} ->
        {:error, :stale}

      {:ok, result} ->
        {:ok, returning_fields(result, returning, opts)}
    end
  end

  def find_one_and_replace(repo, %WriteQuery{} = query, opts) do
    coll = query.coll
    command = query.command
    returning = query.returning
    opts = query.opts ++ opts
    filter = query.query

    if Keyword.get(opts, :delete_matching_documents_before_update_hack) do
      Logger.warning("""
      In order to fulfil this query matching documents must first be deleted.  This could be dangerous and result in data loss!

      To work around this issue you should avoid `on_conflict: :replace_all`.
      """)

      delete(repo, query, opts)
    end

    case query(repo, :find_one_and_replace, [coll, filter, command], opts) do
      {:ok,
       %Mongo.FindAndModifyResult{matched_count: 0, updated_existing: false, upserted_id: nil}} ->
        {:error, :stale}

      {:ok, result} ->
        {:ok, returning_fields(result, returning, opts)}

    end
  end

  @doc """
  Like update_one and update_many except more powerful

  TRY REMOVING
  """
  def update(repo, %WriteQuery{} = query, opts) do
    coll = query.coll
    command = query.command
    opts = query.opts ++ opts
    query = query.query

    case query(repo, :update, [coll, command], opts) do
      # {:ok, %Mongo.UpdateResult{modified_count: 0, matched_count: matched_count, upserted_ids: nil}} ->
      #   {:ok, 0}

      {:ok, %Mongo.UpdateResult{modified_count: 0, upserted_ids: upserted_ids}}
      when is_list(upserted_ids) ->
        {:ok, Enum.count(upserted_ids)}

      {:ok, %Mongo.UpdateResult{modified_count: m} = _result} ->
        {:ok, m}

      {:error, error} ->
        check_constraint_errors(error)
    end
  end

  def insert(repo, %WriteQuery{} = query, opts) do
    coll = query.coll
    command = query.command
    opts = query.opts ++ opts

    case query(repo, :insert_one, [coll, command], opts) do
      {:ok, result} -> {:ok, returning_fields(result, query.returning)}
      {:error, error} -> check_constraint_errors(error, opts)
    end
  end

  # returning_fields/2 extracts the requested returning fields from a Mongo
  # result struct
  defp returning_fields(_result, []), do: []

  defp returning_fields(%Mongo.InsertOneResult{inserted_id: inserted_id}, [:id]),
    do: [id: inserted_id]

  defp returning_fields(%Mongo.FindAndModifyResult{ upserted_id: upserted_id, value: value }, fields, opts) do
    fields
    |> Enum.map(fn
        :id ->
          case Keyword.get(opts, :return_document) do
            :after ->
              {:id, Map.get(value, "_id")}
            _other ->
              {:id, upserted_id}
          end
        field -> {field, Map.get(value, Atom.to_string(field))}

      end)
  end

  def insert_all(repo, %WriteQuery{} = query, opts) do
    coll = query.coll
    command = query.command
    opts = query.opts ++ opts
    on_conflict = opts |> Keyword.get(:on_conflict)

    case query(repo, :insert_many, [coll, command], opts) do
      {:ok, %{inserted_ids: ids}} ->
        {Enum.count(ids), nil}

      {:error, error} ->
        case on_conflict do
          :raise ->
            raise(error)

          :nothing ->
            {0, nil}

          _ ->
            check_constraint_errors(error)
        end
    end
  end

  def command(repo, %CommandQuery{} = query, opts) do
    command = query.command
    opts = query.opts ++ opts

    query(repo, :command!, [command], opts)
  end

  # TODO remove!  connection should be able to figure out whether you need to make two commands for on_conflict: :replace_all
  def query(adapter_meta, :multi, operations, opts) do
    operations
    |> Enum.map(fn {op, args} -> query(adapter_meta, op, args, opts) end)
    |> Enum.at(-1)
  end

  def query(adapter_meta, operation, args, opts) do
    %{pid: pool, telemetry: telemetry, opts: default_opts} = adapter_meta

    args = [pool] ++ args ++ [with_log(telemetry, args, opts ++ default_opts)]
    apply(Mongo, operation, args)
  end

  defp with_log(telemetry, params, opts) do
    [log: &log(telemetry, params, &1, opts)] ++ opts
  end

  defp log({repo, log, event_name}, _params, entry, opts) do
    %{
      connection_time: query_time,
      decode_time: decode_time,
      pool_time: queue_time,
      idle_time: idle_time,
      result: result,
      query: query,
      params: params
    } = entry

    source = Keyword.get(opts, :source)

    params =
      Enum.map(params, fn
        %Ecto.Query.Tagged{value: value} -> value
        value -> value
      end)

    acc = if idle_time, do: [idle_time: idle_time], else: []

    measurements =
      log_measurements(
        [query_time: query_time, decode_time: decode_time, queue_time: queue_time],
        0,
        acc
      )

    metadata = %{
      type: :ecto_sql_query,
      repo: repo,
      result: log_result(result),
      params: params,
      query: format_query(query, params),
      source: source,
      options: Keyword.get(opts, :telemetry_options, [])
    }

    if event_name = Keyword.get(opts, :telemetry_event, event_name) do
      :telemetry.execute(event_name, measurements, metadata)
    end

    case Keyword.get(opts, :log, log) do
      true ->
        Logger.log(
          log,
          fn -> log_iodata(measurements, metadata) end,
          ansi_color: log_color(query)
        )

      false ->
        :ok

      level ->
        Logger.log(
          level,
          fn -> log_iodata(measurements, metadata) end,
          ansi_color: log_color(query)
        )
    end

    :ok
  end

  defp log_measurements([{_, nil} | rest], total, acc),
    do: log_measurements(rest, total, acc)

  defp log_measurements([{key, value} | rest], total, acc),
    do: log_measurements(rest, total + value, [{key, value} | acc])

  defp log_measurements([], total, acc),
    do: Map.new([total_time: total] ++ acc)

  # Currently unused
  defp log_result({:ok, _query, res}), do: {:ok, res}
  defp log_result(other), do: other

  defp log_iodata(measurements, metadata) do
    %{
      params: params,
      query: query,
      result: result,
      source: source
    } = metadata

    [
      "QUERY",
      ?\s,
      log_ok_error(result),
      log_ok_source(source),
      log_time("db", measurements, :query_time, true),
      log_time("decode", measurements, :decode_time, false),
      log_time("queue", measurements, :queue_time, false),
      log_time("idle", measurements, :idle_time, true),
      ?\n,
      query,
      ?\s,
      inspect(params, charlists: false)
    ]
  end

  defp log_ok_error({:ok, _res}), do: "OK"
  defp log_ok_error({:error, _err}), do: "ERROR"

  defp log_ok_source(nil), do: ""
  defp log_ok_source(source), do: " source=#{inspect(source)}"

  defp log_time(label, measurements, key, force) do
    case measurements do
      %{^key => time} ->
        us = System.convert_time_unit(time, :native, :microsecond)
        ms = div(us, 100) / 10

        if force or ms > 0 do
          [?\s, label, ?=, :io_lib_format.fwrite_g(ms), ?m, ?s]
        else
          []
        end

      %{} ->
        []
    end
  end




  defp check_constraint_errors(error, opts) do
    on_conflict = Keyword.get(opts, :on_conflict)


    case on_conflict do
      :nothing -> {:ok, []}

    end
  end

  # At some point in the past it looks like the MongoDB driver switched from
  # returning a single `%Mongo.Error{}` to a `%Mongo.WriteError{}` containing
  # one or more errors in its `write_errors` property.  It looks like
  # `check_constraint_errors` was never really intended to handle that.   JP 2021-08-25.
  defp check_constraint_errors(%Mongo.WriteError{
         write_errors: [%{"code" => 11_000, "errmsg" => msg}]
       }) do
    {:invalid, [unique: extract_index(msg)]}
  end

  defp check_constraint_errors(%Mongo.Error{code: 11_000, message: msg}) do
    {:invalid, [unique: extract_index(msg)]}
  end

  defp check_constraint_errors(other) do
    raise other
  end

  defp extract_index(msg) do
    parts = String.split(msg, [".$", "index: ", " dup "])

    case Enum.reverse(parts) do
      [_, index | _] ->
        String.trim(index)

      _ ->
        raise "failed to extract index from error message: #{inspect(msg)}"
    end
  end

  def format_constraint_error(index) do
    %Mongo.Error{
      message: "ERROR (11000): could not create unique index \"#{index}\" due to duplicated entry"
    }
  end

  defp format_query(%Query{action: :command}, [command]) do
    ["COMMAND " | inspect(command)]
  end

  defp format_query(%Query{action: :find, extra: coll}, [query, projection]) do
    [
      "FIND",
      format_part("coll", coll),
      format_part("query", query),
      format_part("projection", projection)
    ]
  end

  defp format_query(%Query{action: :insert_one, extra: coll}, [doc]) do
    ["INSERT", format_part("coll", coll), format_part("document", doc)]
  end

  defp format_query(%Query{action: :insert_many, extra: coll}, docs) do
    [
      "INSERT",
      format_part("coll", coll),
      format_part("documents", docs),
      format_part("many", true)
    ]
  end

  defp format_query(%Query{action: :update_one, extra: coll}, [filter, update]) do
    [
      "UPDATE",
      format_part("coll", coll),
      format_part("filter", filter),
      format_part("update", update)
    ]
  end

  defp format_query(%Query{action: :update_many, extra: coll}, [filter, update]) do
    [
      "UPDATE",
      format_part("coll", coll),
      format_part("filter", filter),
      format_part("update", update),
      format_part("many", true)
    ]
  end

  defp format_query(%Query{action: :delete_one, extra: coll}, [filter]) do
    ["DELETE", format_part("coll", coll), format_part("filter", filter)]
  end

  defp format_query(%Query{action: :delete_many, extra: coll}, [filter]) do
    [
      "DELETE",
      format_part("coll", coll),
      format_part("filter", filter),
      format_part("many", true)
    ]
  end

  defp format_query(%Query{action: :replace_one, extra: coll}, [filter, doc]) do
    [
      "REPLACE",
      format_part("coll", coll),
      format_part("filter", filter),
      format_part("document", doc)
    ]
  end

  defp format_query(%Query{action: :get_more, extra: coll}, [cursor]) do
    ["GET_MORE", format_part("coll", coll), format_part("cursor_id", cursor)]
  end

  defp format_query(%Query{action: :get_more, extra: coll}, []) do
    ["GET_MORE", format_part("coll", coll), format_part("cursor_id", "")]
  end

  defp format_query(%Query{action: :kill_cursors, extra: _coll}, [cursors]) do
    ["KILL_CURSORS", format_part("cursor_ids", cursors)]
  end

  defp format_query(%Query{action: :kill_cursors, extra: _coll}, []) do
    ["KILL_CURSORS", format_part("cursor_ids", "")]
  end

  defp format_query(%Query{action: :wire_version, extra: _coll}, []) do
    ["WIRE_VERSION", format_part("cursor_ids", "")]
  end

  defp format_part(name, value) do
    [" ", name, "=" | inspect(value)]
  end

  defp log_color(%Query{action: :command}), do: :white
  defp log_color(%Query{action: :find}), do: :cyan
  defp log_color(%Query{action: :insert_one}), do: :green
  defp log_color(%Query{action: :insert_many}), do: :green
  defp log_color(%Query{action: :update_one}), do: :yellow
  defp log_color(%Query{action: :update_many}), do: :yellow
  defp log_color(%Query{action: :delete_many}), do: :red
  defp log_color(%Query{action: :replace_one}), do: :yellow
  defp log_color(%Query{action: :get_more}), do: :cyan
  defp log_color(%Query{action: _}), do: nil
end
