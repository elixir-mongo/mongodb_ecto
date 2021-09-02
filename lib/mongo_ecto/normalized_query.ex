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

    defstruct coll: nil, query: %{}, command: %{}, database: nil, opts: []
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

    # original |> IO.inspect(label: "original")
    # params |> IO.inspect(label: "params")
    # from |> IO.inspect(label: "from")

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

  def insert(%{source: coll, schema: schema, prefix: prefix}, document) do
    command = command(:insert, document, primary_key(schema))

    %WriteQuery{coll: coll, command: command, database: prefix}
  end

  defp upsert(schema_meta, [], set_fields, [], returning, opts) do
    # When both `conflict_targets and `set_on_insert` are empty we can consider this a plain insert.
    # TODO not sure if is correct to use :raise here
    insert(schema_meta, set_fields, {:raise, [], []}, returning, opts)
  end

  # defp upsert(schema_meta, query, set_fields, [], returning, opts) do

  #   insert(schema_meta, set_fields, {:raise, [], []}, returning, opts)
  # end

  defp upsert(%{source: coll, schema: schema}, query, set_fields, set_on_insert_fields, _returning, _opts) do
    pk = primary_key(schema)

    should_replace? = set_fields |> Keyword.has_key?(pk)

    set_on_insert_fields = case Keyword.get(set_fields, pk) do
      nil -> set_on_insert_fields
      v -> set_on_insert_fields |> Keyword.put(pk, v)
    end


    case should_replace? do
      true ->
        replacement_doc = set_fields ++ set_on_insert_fields

        replacement_doc = replacement_doc |> Enum.dedup() |> value(pk, "insert_one for replace_all") |> map_unless_empty()

        delete_doc = Keyword.take(set_fields, [pk]) |> value(pk, "delete_one for replace_all")

        {:multi, [
          {:delete_one, [coll, query]},
          {:insert_one, [coll, replacement_doc]}
        ]}

      false ->


        update = [
          "$set": set_fields |> Keyword.drop([pk]) |> value(pk, "insert command set fields") |> map_unless_empty(),
          "$setOnInsert": set_on_insert_fields |> value(pk, "insert command set on insert fields") |> map_unless_empty()
        ] |> Enum.reject(fn {_k, v} -> v == %{} end) # TODO factor out

        command =
        [
          query: query,
          update: update,
          upsert: true
        ]

        opts = [
          return_document: :after,
          upsert: true
        ]

          # coll filter update opts
        {:find_one_and_update, [coll, query, update], opts}


    end

  end

  def insert(%{ schema: schema } = schema_meta, fields, {[_ | _] = replace_fields, _, conflict_targets}, returning, opts) do
    # IO.inspect(replace_fields, label: "replace_fields")
    # IO.inspect(conflict_targets, label: "conflict targets")


    {set_fields, set_on_insert_fields} = Keyword.split(fields, replace_fields)

    set_on_insert_fields = set_on_insert_fields ++ Keyword.take(set_fields, conflict_targets)

    set_fields = set_fields |> Keyword.drop(conflict_targets)

    pk = primary_key(schema)

    query = fields |> Keyword.take(conflict_targets) |> query(pk)

    # IO.inspect(query, label: "query")
    # IO.inspect(set_fields, label: "set_fields")
    # IO.inspect(set_on_insert_fields, label: "set_on_insert_fields")

    upsert(schema_meta, query, set_fields, set_on_insert_fields, returning, opts)
  end

  def insert(%{ source: coll, schema: schema }, fields, {%Ecto.Query{} = query, values, conflict_targets}, returning, opts) do
    %{ query: find_query } = query |> all(values)

    IO.inspect(fields, label: "fields")


    %{ command: update }  = query |> update_all(Keyword.values(fields) ++ values)

    pk = primary_key(schema)

    # IO.inspect(pk, label: "primary key")

    # need to check for duplicates in $set and $setOnInsert
    update = update
    |> Map.put(:"$setOnInsert", fields |> value(pk, "set on insert fields"))

    # IO.inspect(update, label: "initial update")

    set_fields = Map.get(update, :"$set")

    # ID is a special case

    # [{pkk, pkv}] = fields |> Keyword.take([pk]) |> value(pk, "primary key projection")

    pkk = :_id
    pkv = "$_id"

    projection = %{}

    projection = projection
    |> Map.put(pkk, %{ "$cond": ["$#{pkk}", "$REMOVE", pkv]})


    projection =
      set_fields
      |> Enum.reduce(projection, fn {k, v}, acc ->
        # Map.put(acc, k, %{ "$cond": ["$#{pkk}", fields[k], v]})

        Map.put(acc, k, %{ "$cond": ["$#{pkk}", v, fields[k]]})
      end)

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

    # query = Keyword.take(fields, Keyword.keys(set_fields)) ++ Keyword.take(field s, conflict_targets) ++ Enum.into(find_query, []),
    query = Keyword.take(fields, conflict_targets) ++ Enum.into(find_query, [])

    # command =
    #   [

    #     query: query,
    #     update: update,
    #     upsert: find_query == %{}
    #   ]

    opts = [
      upsert: find_query == %{},
      return_document: :after
    ]

      # coll filter update opts
  {:find_one_and_update, [coll, query, update], opts}
  end

  def insert(%{source: coll, schema: schema}, fields, {:nothing, [], conflict_targets}, _returning, _opts) do
    # command = command(:insert, fields, primary_key(schema), on_conflict)

    if :id in conflict_targets do
      raise "Primary key not allowed as a target?!"
    end


    command =
      fields
      |> value(primary_key(schema), "insert")
      |> map_unless_empty

    {:insert_one, [coll, command]}
  end


  # Default simple case - plain insert with default :raise on_conflict behaviour
  def insert(%{source: coll, schema: schema}, fields, {:raise, [], []}, _returning, _opts) do
    # command = command(:insert, fields, primary_key(schema), on_conflict)

    command =
      fields
      |> value(primary_key(schema), "insert")
      |> map_unless_empty

    {:insert_one, [coll, command]}
  end

  # def insert(%{source: coll, schema: schema, prefix: prefix}, document, {[_ | _] = replace_fields, _, conflict_targets}) do
  #   # No conflict targets have been specified, so in this case we're relying on
  #   # the database to report any conflicts that occur, e.g. via a unique index violation.

  #   command = command(:insert, document, primary_key(schema))

  #   # {set_fields, set_on_insert_fields} = Keyword.split(document, replace_fields)

  #   # filter = Keyword.take(document, conflict_targets)

  #   # %WriteQuery{
  #   #   coll: coll,
  #   #   command: ["$set": set_fields |> normalise_id(), "$setOnInsert": set_on_insert_fields |> normalise_id()],
  #   #   database: nil,
  #   #   opts: [] |> Keyword.put(:upsert, true),
  #   #   query: filter
  #   # }
  # end

  def insert(_meta, _document, on_conflict), do: raise "Unsupported on_conflict: #{inspect(on_conflict)}"

  # def update_all(%Query{} = original, params) do
  #   check_query!(original)

  #   params = List.to_tuple(params)
  #   from = from(original)
  #   coll = coll(from)
  #   query = query(original, params, from)
  #   command = command(:update, original, params, from)

  #   %WriteQuery{coll: coll, query: query, command: command, database: original.prefix}
  # end

  def command(command, opts) do
    %CommandQuery{command: command, database: Keyword.get(opts, :database, nil)}
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
    # fields |> IO.inspect(label: "fields")
    # params |> IO.inspect(label: "params")
    # from |> IO.inspect(label: "from")
    # query |> IO.inspect(label: "query")

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

  defp command(:insert, document, pk) do
    document
    |> value(pk, "insert command")
    |> map_unless_empty
  end

  defp command(:update, values, pk) do
    ["$set": values |> value(pk, "update command") |> map_unless_empty]
  end

  defp command(:insert, document, pk, {[_ | _] = replace_fields, _, conflict_targets}) do

    {set_fields, set_on_insert_fields} = Keyword.split(document, replace_fields)

    # set_fields |> IO.inspect(label: "set fields")
    # set_on_insert_fields |> IO.inspect(label: "set on insert fields")

    [
      "$set": set_fields |> value(pk, "insert command set fields"),
      "$setOnInsert": set_on_insert_fields |> value(pk, "insert command set on insert fields")
    ]
    |> map_unless_empty()
    # |> IO.inspect(label: "query")
  end

  defp command(:insert, document, pk, on_conflict) do
    # IO.inspect(on_conflict, label: "unhandled insert on_conflict")
    document
    # |> IO.inspect(label: "document")
    |> value(pk, "insert command")
    # |> IO.inspect(label: "document after value")
    |> map_unless_empty()
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
    # IO.inspect(expr)
    # IO.inspect(params)
    # IO.inspect(pk)
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

  update = [set: :"$set", inc: :"$inc", push: :"$push", pull: :"$pull"]

  Enum.map(update, fn {key, op} ->
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
