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
    pk = query.pk
    query = query.query

    case query(repo, :find_one_and_update, [coll, query, command], opts) do
      {:ok,
       %Mongo.FindAndModifyResult{matched_count: 0, updated_existing: false, upserted_id: nil}} ->
        {:error, :stale}

      {:ok, result} ->
        {:ok, returning_fields(result, returning, pk, opts)}
    end
  end

  def find_one_and_replace(repo, %WriteQuery{} = query, opts) do
    coll = query.coll
    command = query.command
    returning = query.returning
    opts = query.opts ++ opts
    pk = query.pk
    filter = query.query

    case query(repo, :find_one_and_replace, [coll, filter, command], opts) do
      {:ok,
       %Mongo.FindAndModifyResult{matched_count: 0, updated_existing: false, upserted_id: nil}} ->
        {:error, :stale}

      {:ok, result} ->
        {:ok, returning_fields(result, returning, pk, opts)}
    end
  end

  def update(repo, %WriteQuery{} = query, opts) do
    coll = query.coll
    command = query.command
    opts = query.opts ++ opts

    case query(repo, :update, [coll, command], opts) do
      {:ok, %Mongo.UpdateResult{modified_count: 0, upserted_ids: [_ | _] = upserted_ids}} ->
        {Enum.count(upserted_ids), nil}

      {:ok, %Mongo.UpdateResult{modified_count: modified_count}} ->
        {modified_count, nil}
    end
  end

  def insert(repo, %WriteQuery{} = query, opts) do
    coll = query.coll
    command = query.command
    opts = query.opts ++ opts

    case query(repo, :insert_one, [coll, command], opts) do
      {:ok, result} ->
        {:ok, returning_fields(result, query.returning, query.pk)}

      {:error, error} ->
        check_constraint_errors(repo, query, error, {:insert, [repo, query, opts]}, opts)
    end
  end

  # returning_fields/2 extracts the requested returning fields from a Mongo
  # result struct
  defp returning_fields(result, fields, primary_key, opts \\ [])

  defp returning_fields(_result, [], _primary_key, _opts), do: []

  defp returning_fields(%Mongo.InsertOneResult{inserted_id: inserted_id}, [pk], pk, _opts),
    do: Keyword.put([], pk, inserted_id)

  defp returning_fields(
         %Mongo.FindAndModifyResult{upserted_id: upserted_id, value: value},
         fields,
         pk,
         opts
       ) do
    fields
    |> Enum.map(fn
      ^pk ->
        case Keyword.get(opts, :return_document) do
          :after ->
            {pk, Map.get(value, "_id")}

          _other ->
            {pk, upserted_id}
        end

      field ->
        {field, Map.get(value, Atom.to_string(field))}
    end)
  end

  def insert_all(repo, %WriteQuery{} = query, opts) do
    coll = query.coll
    command = query.command
    opts = query.opts ++ opts

    case query(repo, :insert_many, [coll, command], opts) do
      {:ok, %{inserted_ids: ids}} ->
        {Enum.count(ids), nil}

      {:error, error} ->
        check_constraint_errors(repo, query, error, {:insert_all, [repo, query, opts]}, opts)
    end
  end

  def command(repo, %CommandQuery{} = query, opts) do
    command = query.command
    opts = query.opts ++ opts

    query(repo, :command!, [command], opts)
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

  defp check_constraint_errors(repo, query, error, {retry_function_name, retry_args}, opts) do
    on_conflict = Keyword.get(opts, :on_conflict)

    case on_conflict do
      :nothing ->
        conflict_targets = opts |> Keyword.get(:conflict_target, [])

        %{write_errors: [%{"keyPattern" => key_pattern}]} = error

        conflicting_keys = Map.keys(key_pattern)

        conflict_targets
        |> Enum.each(fn conflict_target ->
          if Atom.to_string(conflict_target) not in conflicting_keys do
            raise error
          end
        end)

        if query.op == :insert_all do
          {0, nil}
        else
          {:ok, []}
        end

      :replace_all ->
        # Here we have to do a song and dance to delete the offending documents and then reattempt the operation.
        # The recommended practice is to avoid :replace_all (and other on_conflict options that result in similar need)

        if allow_unsafe_upserts?(opts) do
          error.write_errors
          |> Enum.each(fn write_error ->
            %{"keyValue" => filter} = write_error

            query(repo, :delete_one, [query.coll, filter], opts)
          end)

          apply(__MODULE__, retry_function_name, retry_args)
        else
          raise """
          `on_conflict: :replace_all` cannot be accomplished in MongoDB without multiple database calls, not least
          because MongoDB does not allow the primary key (`_id`) to be replaced.

          To workaround this issue you may:

          * Use a different `on_conflict` strategy (this is the safest option)
          * If you must use `:replace_all`, you may pass an additional `allow_unsafe_upserts: true` option.

          Passing `allow_unsafe_upserts: true` will cause `mongodb_ecto` to issue multiple database calls in order
          to resolve conflicts.  Since multiple independent calls are involve this cannot be considered safe and
          should be avoided if possible.
          """
        end
    end
  end

  defp allow_unsafe_upserts?(opts) do
    allow_unsafe_upserts_option = :allow_unsafe_upserts
    Application.get_env(:mongodb_ecto, allow_unsafe_upserts_option, false) || Keyword.get(opts, allow_unsafe_upserts_option, false)
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
