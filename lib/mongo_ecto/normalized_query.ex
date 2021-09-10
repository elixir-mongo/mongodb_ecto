defmodule Mongo.Ecto.NormalizedQuery do
  @moduledoc false

  defmodule ReadQuery do
    @moduledoc false

    defstruct coll: nil,
              pk: nil,
              params: {},
              query: %{},
              projection: %{},
              order: %{},
              fields: [],
              database: nil,
              opts: []
  end

  defmodule WriteQuery do
    @moduledoc false

    defstruct op: nil,
              coll: nil,
              query: %{},
              command: %{},
              database: nil,
              returning: [],
              opts: []
  end

  defmodule CommandQuery do
    @moduledoc false

    defstruct command: nil, database: nil, opts: []
  end

  defmodule CountQuery do
    @moduledoc false

    defstruct coll: nil, pk: nil, fields: [], query: %{}, database: nil, opts: []
  end

  defmodule AggregateQuery do
    @moduledoc false

    defstruct coll: nil, pk: nil, fields: [], pipeline: [], database: nil, opts: []
  end

  require Ecto.Query

  alias Ecto.Query
  alias Mongo.Ecto.Conversions

  defmacrop is_op(op) do
    quote do
      is_atom(unquote(op)) and unquote(op) != :^
    end
  end

  def all(original, params) do
    check_query!(original, [:limit, :offset])

    from = from(original)
    params = List.to_tuple(params)
    query = query(original, params, from)

    case projection(original, params, from) do
      {:count, fields} ->
        count(original, query, fields, params, from)

      {:find, projection, fields} ->
        find_all(original, query, projection, fields, params, from)

      {:aggregate, pipeline, fields} ->
        aggregate(original, query, pipeline, fields, params, from)
    end
  end

  defp find_all(original, query, projection, fields, params, {coll, _, pk} = from) do
    %ReadQuery{
      coll: coll,
      pk: pk,
      params: params,
      query: query,
      fields: fields,
      projection: projection,
      order: order(original, from),
      database: original.prefix,
      opts: limit_skip(original, params, from)
    }
  end

  defp count(original, query, fields, params, {coll, _, pk} = from) do
    %CountQuery{
      coll: coll,
      query: query,
      opts: limit_skip(original, params, from),
      pk: pk,
      fields: fields,
      database: original.prefix
    }
  end

  defp aggregate(original, query, pipeline, fields, params, {coll, _, pk} = from) do
    pipeline =
      limit_skip(original, params, from)
      |> Enum.map(fn
        {:limit, value} -> ["$limit": value]
        {:skip, value} -> ["$skip": value]
      end)
      |> Kernel.++(pipeline)

    pipeline = if query != %{}, do: [["$match": query] | pipeline], else: pipeline

    %AggregateQuery{
      coll: coll,
      pipeline: pipeline,
      pk: pk,
      fields: fields,
      database: original.prefix
    }
  end

  def update_all(%Query{} = original, params) do
    check_query!(original)

    params = List.to_tuple(params)
    from = from(original)
    coll = coll(from)
    query = query(original, params, from)
    command = command(:update, original, params, from)

    %WriteQuery{coll: coll, query: query, command: command, database: original.prefix}
  end

  def update_one(%{source: coll, prefix: prefix, schema: schema}, fields, filter) do
    command = command(:update, fields, primary_key(schema))
    query = query(filter, primary_key(schema))

    %WriteQuery{coll: coll, query: query, database: prefix, command: command}
  end

  def update(%{source: coll, prefix: prefix, schema: schema}, fields, filter) do
    command = command(:update, fields, primary_key(schema))
    query = query(filter, primary_key(schema))

    %WriteQuery{coll: coll, query: query, database: prefix, command: command}
  end

  def delete_all(%Query{} = original, params) do
    check_query!(original)

    params = List.to_tuple(params)
    from = from(original)
    coll = coll(from)
    query = query(original, params, from)

    %WriteQuery{coll: coll, query: query, database: original.prefix}
  end

  def delete(%{source: coll, schema: schema, prefix: prefix}, filter) do
    query = query(filter, primary_key(schema))

    %WriteQuery{coll: coll, query: query, database: prefix}
  end

  # # TODO this insert should no longer be used
  def insert(%{source: coll, schema: schema, prefix: prefix}, document) do
    command = command(:insert, document, primary_key(schema))

    %WriteQuery{coll: coll, command: command, database: prefix}
  end

  ####### JP START

  # `:raise` is the default behaviour when on_conflict is not specified. In this
  # case we assume that any conflict will be raised by the driver (e.g.
  # duplicate value on a uniquely indexed field)
  def insert(schema_meta, fields, {:raise, [], []}, returning, opts),
    do: plain_insert(schema_meta, fields, returning)

  # When a user specifies `on_conflict: :nothing` with no conflict targets then
  # we can treat this as a plain insert.  It's expected that exceptions from the
  # driver will be suppressed elsewhere as appropriate.
  def insert(schema_meta, fields, {:nothing, [], []}, returning, _opts),
    do: plain_insert(schema_meta, fields, returning)

  # User specified `on_conflict: :nothing` with specified fields that must be
  # checked for conflicts.
  def insert(
        %{source: coll, schema: schema, prefix: prefix},
        fields,
        {:nothing, [], conflict_targets},
        returning,
        _opts
      ) do
    check_conflict_targets!(schema, conflict_targets)

    # User has specified `conflict_targets` so we need to use a
    # `find_one_and_update` operation instead of a plain insert.
    #
    # We'll use the conflict targets to construct a query that can be used to
    # find existing matching documents.
    op = :find_one_and_update

    pk = primary_key(schema)

    # Use `conflict_targets` to create a query that will match any existing documents
    query = query(fields, conflict_targets, pk)
    command = command(:update, [], fields, pk)

    # * `upsert: true` creates an existing document if none already exists.
    # * `return_document: :after` causes the upserted document to be returned,
    #   which we need since we need to know the ID of the inserted or updated
    #   document.  Ecto expects updates **not** to return an ID but Mongo always
    #   does (seems like a rare moment where Mongo has slightly clearer support
    #   than SQL).  It's expected that this discrepency is massaged out
    #   elsewhere.
    opts = [
      upsert: true,
      # return_document: :after
    ]

    %WriteQuery{
      op: op,
      coll: coll,
      query: query,
      command: command,
      database: prefix,
      returning: returning,
      opts: opts
    }
  end

  # User specified a list of fields to replace and conflict targets to check.
  # This gets called if the user has specified an `on_conflict` value of
  # `:replace_all`, `{:replace, fields}` or `{:replace_all_except, fields}`
  #
  # Ecto wants us to be able to support the replacement of the `id` field which
  # MongoDB forbids, so this gets a bit tricky.  When the user wishes to replace
  # the `id` field then the only way I have found to make this work is to
  # perform a delete followed by an insert; not ideal since data loss could
  # occur.  We can't do an insert followed by a delete instead because the
  # inserted document could contain conflicts with the document that is about to
  # be deleted.
  def insert(
        %{schema: schema} = schema_meta,
        fields,
        {[_ | _] = replace_fields, _, conflict_targets},
        returning,
        opts
      ) do
    pk = primary_key(schema)
    query = query(fields, conflict_targets, pk)

    if pk in replace_fields do
      replace(schema_meta, query, fields, returning)
    else
      {set_fields, set_on_insert_fields} = upsert_fields(fields, replace_fields, conflict_targets)

      upsert(schema_meta, query, set_fields, set_on_insert_fields, returning, opts)
    end
  end

  # User specified a query to perform in the event of a conflict.
  #
  # This is achieved using the mongo `$project` operator.  Undoubtedly this
  # could be refactored to be clearer code wise but for now since `$project` is
  # only used here a long function will suffice.
  def insert(
        %{source: coll, schema: schema, prefix: prefix},
        fields,
        {%Ecto.Query{} = query, values, conflict_targets},
        returning,
        _opts
      ) do
    check_conflict_targets!(schema, conflict_targets)

    pk = primary_key(schema)
    from = from(query)

    # Create a query for finding existing documents based on the conflict targets
    find_query_values =
      fields |> Keyword.take(conflict_targets) |> Keyword.values() |> List.to_tuple()

    find_query =
      query
      |> filtering_conflict_targets(fields, conflict_targets)
      |> query(find_query_values, from)

    # If the query has specified a where then this cannot be an upsert; it's
    # expected that a non-matching query return no results (and be reported as
    # stale by Ecto)
    upsert = query.wheres == []

    %{"$set": set_fields} =
      command(:update, query, List.to_tuple(Keyword.values(fields) ++ values), from)

    # The behaviour we want is for different values to be set on the database
    # depending on whether this is an insert or an update.  In Mongo land we
    # might tend to reach for `$set` and `$setOnInsert`, however Mongo disallows
    # us from having the same fields in both `$set` and `$setOnInsert`, so that
    # won't work.
    #
    # The workaround implemented below is to use the `$project` aggregate
    # operator along with `$cond`.  `$cond` allows us to check for the presence
    # of an `_id` field, so we can `$project` different fields depending on
    # whether this was an insert or an update.

    # ID is a special case
    pkk = :_id
    pkv = "$_id"

    projection = %{}

    # If an existing document matches `find_query`, then `_id` will be present.
    # If we specify an `_id` in the projection of an **existing** document then
    # Mongo complains, so we must omit it in this case.  There's a special
    # variable `$REMOVE` for this purpose which tells Mongo to omit that field
    # from the projection.
    projection =
      projection
      |> Map.put(pkk, %{"$cond": ["$#{pkk}", "$REMOVE", pkv]})

    # set_fields are either set to the value from the query (in the event of an
    # update) or the value in fields (in the event of an insert)
    projection =
      set_fields
      |> Enum.reduce(projection, fn {k, v}, acc ->
        Map.put(acc, k, %{"$cond": ["$#{pkk}", v, fields[k]]})
      end)

    # All other fields are just plain "set" operation.  Because we don't want
    # them to conflict with the set vs. set_on_insert operations above, we can
    # use `Map.put_new` to ensure there are no duplicates.
    projection =
      fields
      |> value(pk, "projection")
      |> Enum.reduce(projection, fn {k, v}, acc ->
        Map.put_new(acc, k, v)
      end)

    update = [
      %{
        "$project": projection
      }
    ]

    opts = [
      upsert: upsert,
      return_document: :after
    ]

    %WriteQuery{
      op: :find_one_and_update,
      coll: coll,
      database: prefix,
      query: find_query,
      command: update,
      returning: returning,
      opts: opts
    }
  end

  defp plain_insert(%{source: coll, schema: schema, prefix: prefix}, fields, returning) do
    command = command(:insert, fields, primary_key(schema))

    op =
      case fields do
        [[_ | _] | _] -> :insert_all
        _ -> :insert
      end

    %WriteQuery{op: op, coll: coll, command: command, database: prefix, returning: returning}
  end

  defp replace(%{source: coll, schema: schema, prefix: prefix}, query, fields, returning) do
    pk = primary_key(schema)

    opts = [
      upsert: true,
      return_document: :after,
      delete_matching_documents_before_update_hack: true
    ]

    %WriteQuery{
      op: :find_one_and_replace,
      coll: coll,
      database: prefix,
      query: query,
      command: fields |> value(pk, "replace"),
      returning: returning,
      opts: opts
    }
  end

  # When both `conflict_targets and `set_on_insert` are empty we can consider this a plain insert.
  defp upsert(schema_meta, [], set_fields, [], returning, _opts),
    do: plain_insert(schema_meta, set_fields, returning)

  defp upsert(
         %{source: coll, schema: schema, prefix: prefix},
         query,
         set_fields,
         set_on_insert_fields,
         returning,
         _opts
       ) do
    pk = primary_key(schema)

    update = command(:update, set_fields, set_on_insert_fields, pk)

    opts = [
      return_document: :after,
      upsert: true
    ]

    %WriteQuery{
      op: :find_one_and_update,
      coll: coll,
      database: prefix,
      query: query,
      command: update,
      returning: returning,
      opts: opts
    }
  end

  def command(command, opts) do
    %CommandQuery{command: command, database: Keyword.get(opts, :database)}
  end

  # Splits a list of fields into individual set and set_on_insert lists
  defp upsert_fields(fields, replace_fields, conflict_targets) do
    fields
    |> Keyword.split(replace_fields)
    |> then(fn {set_fields, set_on_insert_fields} ->
      {
        set_fields |> Keyword.drop(conflict_targets),
        set_on_insert_fields ++ Keyword.take(set_fields, conflict_targets)
      }
    end)
  end

  defp filtering_conflict_targets(%Ecto.Query{} = query, fields, conflict_targets) do
    conflict_field_filters = fields |> Keyword.take(conflict_targets)

    Ecto.Query.from(u in query, where: ^conflict_field_filters)
  end

  defp check_conflict_targets!(schema, conflict_targets) do
    # I can't find this documented anywhere but the Ecto integration tests seem
    # to expect an error to be raised in the event that the primary key is
    # specified in `conflict_targets`.  2021-09-02 JP.
    if primary_key(schema) in conflict_targets do
      raise "Invalid conflict targets #{inspect(conflict_targets)}"
    end
  end

  defp from(%Query{from: %{source: {coll, model}}}) do
    {coll, model, primary_key(model)}
  end

  defp from(%Query{from: %{source: %Ecto.SubQuery{}}}) do
    raise ArgumentError, "MongoDB does not support subqueries"
  end

  @aggregate_ops [:min, :max, :sum, :avg]
  @special_ops [:count | @aggregate_ops]

  defp projection(%Query{select: nil}, _params, _from), do: {:find, %{}, []}

  defp projection(
         %Query{select: %Query.SelectExpr{fields: fields} = _select} = query,
         params,
         from
       ) do
    projection(fields, params, from, query, %{}, [])
  end

  defp projection([], _params, _from, _query, pacc, facc), do: {:find, pacc, Enum.reverse(facc)}

  # TODO this projection function is the same as the one below with a different pattern
  defp projection(
         [{:&, _, [0]} = field | rest],
         params,
         {_, nil, _} = from,
         query,
         _pacc,
         facc
       ) do
    facc =
      case projection(rest, params, from, query, %{}, [field | facc]) do
        {:find, _, facc} ->
          facc

        _other ->
          error(
            query,
            "select clause supports only one of the special functions: `count`, `min`, `max`"
          )
      end

    {:find, %{}, facc}
  end

  defp projection(
         [{:&, _, [0, nil, _]} = field | rest],
         params,
         {_, nil, _} = from,
         query,
         _pacc,
         facc
       ) do
    # Model is nil, we want empty projection, but still extract fields
    facc =
      case projection(rest, params, from, query, %{}, [field | facc]) do
        {:find, _, facc} ->
          facc

        _other ->
          error(
            query,
            "select clause supports only one of the special functions: `count`, `min`, `max`"
          )
      end

    {:find, %{}, facc}
  end

  defp projection(
         [{:&, _, [0, nil, _]} = field | rest],
         params,
         {_, model, pk} = from,
         query,
         pacc,
         facc
       ) do
    pacc = Enum.into(model.__schema__(:fields), pacc, &{field(&1, pk), true})
    facc = [field | facc]

    projection(rest, params, from, query, pacc, facc)
  end

  defp projection(
         [{:&, _, [0, fields, _]} = field | rest],
         params,
         {_, _model, pk} = from,
         query,
         pacc,
         facc
       ) do
    pacc = Enum.into(fields, pacc, &{field(&1, pk), true})
    facc = [field | facc]

    projection(rest, params, from, query, pacc, facc)
  end

  defp projection([%Ecto.Query.Tagged{value: value} | rest], params, from, query, pacc, facc) do
    {_, model, pk} = from

    pacc = Enum.into(model.__schema__(:fields), pacc, &{field(&1, pk), true})
    facc = [{:field, pk, value} | facc]

    projection(rest, params, from, query, pacc, facc)
  end

  defp projection([{{:., _, [_, name]}, _, _} = field | rest], params, from, query, pacc, facc) do
    {_, _, pk} = from

    # Projections use names as in database, fields as in models
    pacc = Map.put(pacc, field(name, pk), true)
    facc = [{:field, name, field} | facc]
    projection(rest, params, from, query, pacc, facc)
  end

  # Keyword and interpolated fragments
  defp projection([{:fragment, _, [args]} = field | rest], params, from, query, pacc, facc)
       when is_list(args) or tuple_size(args) == 3 do
    {_, _, pk} = from

    pacc =
      args
      |> value(params, pk, query, "select clause")
      |> Enum.into(pacc)

    facc = [field | facc]

    projection(rest, params, from, query, pacc, facc)
  end

  defp projection([{:count, _, [_]} = field], _params, _from, _query, pacc, _facc)
       when pacc == %{} do
    {:count, [{:field, :value, field}]}
  end

  defp projection([{:count, _, [name, :distinct]} = field], _params, from, query, _pacc, _facc) do
    {_, _, pk} = from
    name = field(name, pk, query, "select clause")
    field = {:field, :value, field}

    {:aggregate, [["$group": [_id: "$#{name}"]], ["$group": [_id: nil, value: ["$sum": 1]]]],
     [field]}
  end

  defp projection([{op, _, [name]} = field], _params, from, query, pacc, _facc)
       when pacc == %{} and op in @aggregate_ops do
    {_, _, pk} = from
    name = field(name, pk, query, "select clause")
    field = {:field, :value, field}
    {:aggregate, [["$group": [_id: nil, value: [{"$#{op}", "$#{name}"}]]]], [field]}
  end

  defp projection([{op, _, _} | _rest], _params, _from, query, _pacc, _facc)
       when op in @special_ops do
    error(
      query,
      "select clause supports only one of the special functions: `count`, `min`, `max`"
    )
  end

  defp projection([{op, _, _} | _rest], _params, _from, query, _pacc, _facc) when is_op(op) do
    error(query, "select clause")
  end

  defp limit_skip(%Query{limit: limit, offset: offset} = query, params, {_, _, pk}) do
    [
      limit: offset_limit(limit, params, pk, query, "limit clause"),
      skip: offset_limit(offset, params, pk, query, "offset clause")
    ]
    |> Enum.reject(&is_nil(elem(&1, 1)))
  end

  defp coll({coll, _model, _pk}), do: coll

  defp query(%Query{wheres: wheres} = query, params, {_coll, _model, pk}) do
    wheres
    |> Enum.map(fn %Query.BooleanExpr{expr: expr} ->
      pair(expr, params, pk, query, "where clause")
    end)
    |> :lists.flatten()
    |> merge_keys(query, "where clause")
    |> map_unless_empty
  end

  defp query([{_, _} | _] = fields, keys, pk) do
    fields
    |> Keyword.take(keys)
    |> query(pk)
  end

  defp query(filter, pk) do
    filter |> value(pk, "where clause") |> map_unless_empty
  end

  defp order(%Query{order_bys: order_bys} = query, {_coll, _model, pk}) do
    order_bys
    |> Enum.flat_map(fn %Query.QueryExpr{expr: expr} ->
      Enum.map(expr, &order_by_expr(&1, pk, query))
    end)
    |> map_unless_empty
  end

  defp command(:update, %Query{updates: updates} = query, params, {_coll, _model, pk}) do
    updates
    |> Enum.flat_map(fn %Query.QueryExpr{expr: expr} ->
      Enum.map(expr, fn {key, value} ->
        value = value |> value(params, pk, query, "update clause")
        {update_op(key, query), value}
      end)
    end)
    |> merge_keys(query, "update clause")
  end

  defp command(:update, [], set_on_insert_fields, pk), do: set_on_insert(set_on_insert_fields, pk)

  defp command(:update, set_fields, set_on_insert_fields, pk) do
    # Set fields are the same as doing a plain update with `$set`
    set_command = command(:update, set_fields, pk)

    # These fields are only applied if the operation results in an insert.
    set_on_insert_command = set_on_insert(set_on_insert_fields, pk)

    set_command ++ set_on_insert_command
  end

  defp command(:insert, document, pk) do
    document
    |> value(pk, "insert command")
    |> map_unless_empty
  end

  defp command(:update, values, pk) do
    ["$set": values |> value(pk, "update command") |> map_unless_empty]
  end

  defp set_on_insert(fields, pk) do
    [
      "$setOnInsert": fields |> value(pk, "update command (set on insert)") |> map_unless_empty()
    ]
  end

  defp offset_limit(nil, _params, _pk, _query, _where), do: nil

  defp offset_limit(%Query.QueryExpr{expr: expr}, params, pk, query, where),
    do: value(expr, params, pk, query, where)

  defp primary_key(nil), do: nil

  defp primary_key(schema) do
    case schema.__schema__(:primary_key) do
      [] ->
        nil

      [pk] ->
        pk

      keys ->
        raise ArgumentError,
              "MongoDB adapter does not support multiple primary keys " <>
                "and #{inspect(keys)} were defined in #{inspect(schema)}."
    end
  end

  defp order_by_expr({:asc, expr}, pk, query), do: {field(expr, pk, query, "order clause"), 1}
  defp order_by_expr({:desc, expr}, pk, query), do: {field(expr, pk, query, "order clause"), -1}

  @maybe_disallowed ~w(distinct lock joins group_bys havings limit offset)a
  @query_empty_values %Ecto.Query{} |> Map.take(@maybe_disallowed)

  defp check_query!(query, allow \\ []) do
    @query_empty_values
    |> Map.drop(allow)
    |> Enum.each(fn {element, empty} ->
      check(
        Map.get(query, element),
        empty,
        query,
        "MongoDB adapter does not support #{element} clause in this query"
      )
    end)
  end

  defp check(expr, expr, _, _), do: nil
  defp check(_, _, query, message), do: raise(Ecto.QueryError, query: query, message: message)

  defp value(expr, pk, place) do
    case Conversions.from_ecto_pk(expr, pk) do
      {:ok, value} -> value
      :error -> error(place)
    end
  end

  defp value(expr, params, pk, query, place) do
    case Conversions.inject_params(expr, params, pk) do
      {:ok, value} -> value
      :error -> error(query, place)
    end
  end

  defp field(pk, pk), do: :_id
  defp field(key, _), do: key

  defp field({{:., _, [{:&, _, [0]}, field]}, _, []}, pk, _query, _place), do: field(field, pk)
  defp field(_expr, _pk, query, place), do: error(query, place)

  defp map_unless_empty([]), do: %{}
  defp map_unless_empty(list), do: list

  defp merge_keys(keyword, query, place) do
    Enum.reduce(keyword, %{}, fn {key, value}, acc ->
      Map.update(acc, key, value, fn
        old when is_list(old) -> old ++ value
        _ -> error(query, place)
      end)
    end)
  end

  update_ops = [set: :"$set", inc: :"$inc", push: :"$push", pull: :"$pull"]

  Enum.map(update_ops, fn {key, op} ->
    def update_op(unquote(key), _query), do: unquote(op)
  end)

  def update_op(_, query), do: error(query, "update clause")

  binary_ops = [>: :"$gt", >=: :"$gte", <: :"$lt", <=: :"$lte", !=: :"$ne", in: :"$in"]
  bool_ops = [and: :"$and", or: :"$or"]

  @binary_ops Keyword.keys(binary_ops)
  @bool_ops Keyword.keys(bool_ops)

  Enum.map(binary_ops, fn {op, mongo_op} ->
    defp binary_op(unquote(op)), do: unquote(mongo_op)
  end)

  Enum.map(bool_ops, fn {op, mongo_op} ->
    defp bool_op(unquote(op)), do: unquote(mongo_op)
  end)

  defp mapped_pair_or_value({op, _, _} = tuple, params, pk, query, place) when is_op(op) do
    List.wrap(pair(tuple, params, pk, query, place))
  end

  defp mapped_pair_or_value(value, params, pk, query, place) do
    value(value, params, pk, query, place)
  end

  defp pair({op, _, args}, params, pk, query, place) when op in @bool_ops do
    args = Enum.map(args, &mapped_pair_or_value(&1, params, pk, query, place))
    {bool_op(op), args}
  end

  defp pair({:is_nil, _, [expr]}, _, pk, query, place) do
    {field(expr, pk, query, place), nil}
  end

  defp pair({:==, _, [left, right]}, params, pk, query, place) do
    {field(left, pk, query, place), value(right, params, pk, query, place)}
  end

  defp pair({:in, _, [left, {:^, _, [0, 0]}]}, _params, pk, query, place) do
    {field(left, pk, query, place), ["$in": []]}
  end

  defp pair({:in, _, [left, {:^, _, [ix, len]}]}, params, pk, query, place) do
    args =
      ix..(ix + len - 1)
      |> Enum.map(&elem(params, &1))
      |> Enum.map(&value(&1, params, pk, query, place))

    {field(left, pk, query, place), ["$in": args]}
  end

  defp pair({:in, _, [lhs, {{:., _, _}, _, _} = rhs]}, params, pk, query, place) do
    {field(rhs, pk, query, place), value(lhs, params, pk, query, place)}
  end

  defp pair({op, _, [left, right]}, params, pk, query, place) when op in @binary_ops do
    {field(left, pk, query, place), [{binary_op(op), value(right, params, pk, query, place)}]}
  end

  defp pair({:not, _, [{:in, _, [left, {:^, _, [ix, len]}]}]}, params, pk, query, place) do
    args =
      ix..(ix + len - 1)
      |> Enum.map(&elem(params, &1))
      |> Enum.map(&value(&1, params, pk, query, place))

    {field(left, pk, query, place), ["$nin": args]}
  end

  defp pair({:not, _, [{:in, _, [left, right]}]}, params, pk, query, place) do
    {field(left, pk, query, place), ["$nin": value(right, params, pk, query, place)]}
  end

  defp pair({:not, _, [{:is_nil, _, [expr]}]}, _, pk, query, place) do
    {field(expr, pk, query, place), ["$ne": nil]}
  end

  defp pair({:not, _, [{:==, _, [left, right]}]}, params, pk, query, place) do
    {field(left, pk, query, place), ["$ne": value(right, params, pk, query, place)]}
  end

  defp pair({:not, _, [expr]}, params, pk, query, place) do
    {:"$not", [pair(expr, params, pk, query, place)]}
  end

  defp pair({:^, _, _} = expr, params, pk, query, place) do
    case value(expr, params, pk, query, place) do
      bool when is_boolean(bool) ->
        boolean_query_hack_pair(bool)

      _value ->
        error(query, place)
    end
  end

  # Keyword or embedded fragment
  defp pair({:fragment, _, [args]}, params, pk, query, place)
       when is_list(args) or tuple_size(args) == 3 do
    value(args, params, pk, query, place)
  end

  defp pair(bool, _params, _pk, _query, _place) when is_boolean(bool) do
    boolean_query_hack_pair(bool)
  end

  defp pair(_expr, _params, _pk, query, place) do
    error(query, place)
  end

  defp boolean_query_hack_pair(bool) do
    {:_id, ["$exists": bool]}
  end

  defp error(query, place) do
    raise Ecto.QueryError,
      query: query,
      message: "Invalid expression for MongoDB adapter in #{place}"
  end

  defp error(place) do
    raise ArgumentError, "Invalid expression for MongoDB adapter in #{place}"
  end
end
