defmodule Mongo.Ecto.NormalizedQuery do
  @moduledoc false

  defmodule ReadQuery do
    @moduledoc false

    defstruct coll: nil, pk: nil, params: {}, query: %{}, projection: %{},
              order: %{}, fields: [], database: nil, opts: []
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

  alias Mongo.Ecto.Conversions
  alias Ecto.Query.Tagged
  alias Ecto.Query

  defmacrop is_op(op) do
    quote do
      is_atom(unquote(op)) and unquote(op) != :^
    end
  end

  def all(%Query{} = original, params) do
    check_query!(original, [:limit, :offset])

    from   = from(original)
    params = List.to_tuple(params)
    query  = query(original, params, from)

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
    %ReadQuery{coll: coll, pk: pk, params: params, query: query, fields: fields,
               projection: projection, order: order(original, from),
               database: original.prefix, opts: limit_skip(original, params, from)}
  end

  defp count(original, query, fields, params, {coll, _, pk} = from) do
    %CountQuery{coll: coll, query: query, opts: limit_skip(original, params, from),
                pk: pk, fields: fields, database: original.prefix}
  end

  defp aggregate(original, query, pipeline, fields, params, {coll, _, pk} = from) do
    pipeline =
      limit_skip(original, params, from)
      |> Enum.map(fn
        {:limit, value} -> ["$limit": value]
        {:skip,  value} -> ["$skip":  value]
      end)
      |> Kernel.++(pipeline)

    pipeline =
      if query != %{}, do: [["$match": query] | pipeline], else: pipeline

    %AggregateQuery{coll: coll, pipeline: pipeline, pk: pk, fields: fields,
                    database: original.prefix}
  end

  def update_all(%Query{} = original, params) do
    check_query!(original)

    params  = List.to_tuple(params)
    from    = from(original)
    coll    = coll(from)
    query   = query(original, params, from)
    command = command(:update, original, params, from)

    %WriteQuery{coll: coll, query: query, command: command, database: original.prefix}
  end

  def update(%{source: {prefix, coll}, schema: schema}, fields, filter) do
    command = command(:update, fields, primary_key(schema))
    query   = query(filter, primary_key(schema))

    %WriteQuery{coll: coll, query: query, database: prefix, command: command}
  end

  def delete_all(%Query{} = original, params) do
    check_query!(original)

    params = List.to_tuple(params)
    from   = from(original)
    coll   = coll(from)
    query  = query(original, params, from)

    %WriteQuery{coll: coll, query: query, database: original.prefix}
  end

  def delete(%{source: {prefix, coll}, schema: schema}, filter) do
    query = query(filter, primary_key(schema))

    %WriteQuery{coll: coll, query: query, database: prefix}
  end

  def insert(%{source: {prefix, coll}, schema: schema}, document) do
    command = command(:insert, document, primary_key(schema))

    %WriteQuery{coll: coll, command: command, database: prefix}
  end

  def command(command, opts) do
    %CommandQuery{command: command, database: Keyword.get(opts, :database, nil)}
  end

  defp from(%Query{from: {coll, model}}) do
    {coll, model, primary_key(model)}
  end

  defp from(%Query{from: %Ecto.SubQuery{}}) do
    raise ArgumentError, "MongoDB does not support subqueries"
  end

  @aggregate_ops [:min, :max, :sum, :avg]
  @special_ops [:count | @aggregate_ops]

  defp projection(%Query{select: nil}, _params, _from),
    do: {:find, %{}, []}
  defp projection(%Query{select: %Query.SelectExpr{fields: fields}} = query, params, from),
    do: projection(fields, params, from, query, %{}, [])

  defp projection([], _params, _from, _query, pacc, facc),
    do: {:find, pacc, Enum.reverse(facc)}
  defp projection([{:&, _, [0, nil, _]} = field | rest], params, {_, nil, _} = from, query, _pacc, facc) do
    # Model is nil, we want empty projection, but still extract fields
    facc =
      case projection(rest, params, from, query, %{}, [field | facc]) do
        {:find, _, facc} ->
          facc
        _other ->
          error(query, "select clause supports only one of the special functions: `count`, `min`, `max`")
      end
    {:find, %{}, facc}
  end
  defp projection([{:&, _, [0, nil, _]} = field | rest], params, {_, model, pk} = from, query, pacc, facc) do
    pacc = Enum.into(model.__schema__(:fields), pacc, &{field(&1, pk), true})
    facc = [field | facc]

    projection(rest, params, from, query, pacc, facc)
  end
  defp projection([{:&, _, [0, fields, _]} = field | rest], params, {_, model, pk} = from, query, pacc, facc) do
    pacc = Enum.into(fields, pacc, &{field(&1, pk), true})
    facc = [field | facc]

    projection(rest, params, from, query, pacc, facc)
  end
  defp projection([{{:., _, [_, name]}, _, _} = field| rest], params, from, query, pacc, facc) do
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
  defp projection([{:count, _, [_]} = field], _params, _from, _query, pacc, _facc) when pacc == %{} do
    {:count, [{:field, :value, field}]}
  end
  defp projection([{:count, _, [name, :distinct]} = field], _params, from, query, _pacc, _facc) do
    {_, _, pk} = from
    name  = field(name, pk, query, "select clause")
    field = {:field, :value, field}
    {:aggregate, [["$group": [_id: "$#{name}"]], ["$group": [_id: nil, value: ["$sum": 1]]]], [field]}
  end
  defp projection([{op, _, [name]} = field], _params, from, query, pacc, _facc) when pacc == %{} and op in @aggregate_ops do
    {_, _, pk} = from
    name  = field(name, pk, query, "select clause")
    field = {:field, :value, field}
    {:aggregate, [["$group": [_id: nil, value: [{"$#{op}", "$#{name}"}]]]], [field]}
  end
  defp projection([{op, _, _} | _rest], _params, _from, query, _pacc, _facc) when op in @special_ops do
    error(query, "select clause supports only one of the special functions: `count`, `min`, `max`")
  end
  defp projection([{op, _, _} | _rest], _params, _from, query, _pacc, _facc) when is_op(op) do
    error(query, "select clause")
  end

  defp limit_skip(%Query{limit: limit, offset: offset} = query, params, {_, _, pk}) do
    [limit: offset_limit(limit, params, pk, query, "limit clause"),
     skip: offset_limit(offset, params, pk, query, "offset clause")]
    |> Enum.reject(&is_nil(elem(&1, 1)))
  end

  defp coll({coll, _model, _pk}), do: coll

  defp query(%Query{wheres: wheres} = query, params, {_coll, _model, pk}) do
    wheres
    |> Enum.map(fn %Query.BooleanExpr{expr: expr} ->
      pair(expr, params, pk, query, "where clause")
    end)
    |> :lists.flatten
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

  defp both_nil(nil, nil), do: true
  defp both_nil(_, _), do: false

  defp offset_limit(nil, _params, _pk, _query, _where),
    do: nil
  defp offset_limit(%Query.QueryExpr{expr: expr}, params, pk, query, where),
    do: value(expr, params, pk, query, where)

  defp primary_key(nil),
    do: nil
  defp primary_key(schema) do
    case schema.__schema__(:primary_key) do
      []   -> nil
      [pk] -> pk
      keys ->
        raise ArgumentError, "MongoDB adapter does not support multiple primary keys " <>
          "and #{inspect keys} were defined in #{inspect schema}."
    end
  end

  defp order_by_expr({:asc,  expr}, pk, query),
    do: {field(expr, pk, query, "order clause"),  1}
  defp order_by_expr({:desc, expr}, pk, query),
    do: {field(expr, pk, query, "order clause"), -1}

  @maybe_disallowed ~w(distinct lock joins group_bys havings limit offset)a
  @query_empty_values %Ecto.Query{} |> Map.take(@maybe_disallowed)

  defp check_query!(query, allow \\ []) do
    @query_empty_values
    |> Map.drop(allow)
    |> Enum.each(fn {element, empty} ->
      check(Map.get(query, element), empty, query,
            "MongoDB adapter does not support #{element} clause in this query")
    end)
  end

  defp check(expr, expr, _, _),
    do: nil
  defp check(_, _, query, message),
    do: raise(Ecto.QueryError, query: query, message: message)

  defp value(expr, pk, place) do
    case Conversions.from_ecto_pk(expr, pk) do
      {:ok, value} -> value
      :error       -> error(place)
    end
  end

  defp value(expr, params, pk, query, place) do
    case Conversions.inject_params(expr, params, pk) do
      {:ok, value} -> value
      :error       -> error(query, place)
    end
  end

  defp field(pk, pk), do: :_id
  defp field(key, _), do: key

  defp field({{:., _, [{:&, _, [0]}, field]}, _, []}, pk, _query, _place),
    do: field(field, pk)
  defp field(_expr, _pk, query, place),
    do: error(query, place)

  defp map_unless_empty([]), do: %{}
  defp map_unless_empty(list), do: list

  defp merge_keys(keyword, query, place) do
    Enum.reduce(keyword, %{}, fn {key, value}, acc ->
      Map.update(acc, key, value, fn
        old when is_list(old) -> old ++ value
        _                     -> error(query, place)
      end)
    end)
  end

  update = [set: :"$set", inc: :"$inc", push: :"$push", pull: :"$pull"]

  Enum.map(update, fn {key, op} ->
    def update_op(unquote(key), _query), do: unquote(op)
  end)

  def update_op(_, query), do: error(query, "update clause")

  binary_ops =
    [>: :"$gt", >=: :"$gte", <: :"$lt", <=: :"$lte", !=: :"$ne", in: :"$in"]
  bool_ops =
    [and: :"$and", or: :"$or"]

  @binary_ops Keyword.keys(binary_ops)
  @bool_ops Keyword.keys(bool_ops)

  Enum.map(binary_ops, fn {op, mongo_op} ->
    defp binary_op(unquote(op)), do: unquote(mongo_op)
  end)

  Enum.map(bool_ops, fn {op, mongo_op} ->
    defp bool_op(unquote(op)), do: unquote(mongo_op)
  end)

  defp mapped_pair_or_value({op, _, _} = tuple, params, pk, query, place) when is_op(op) do
    [pair(tuple, params, pk, query, place)]
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
  defp pair({:in, _, [left, {:^, _, [0, 0]}]}, params, pk, query, place) do
    {field(left, pk, query, place), ["$in": []]}
  end
  defp pair({:in, _, [left, {:^, _, [ix, len]}]}, params, pk, query, place) do
    args =
      ix..ix+len-1
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
      ix..ix+len-1
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
    raise Ecto.QueryError, query: query,
      message: "Invalid expression for MongoDB adapter in #{place}"
  end
  defp error(place) do
    raise ArgumentError, "Invalid expression for MongoDB adapter in #{place}"
  end
end
