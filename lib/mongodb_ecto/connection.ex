defmodule MongodbEcto.Connection do

  @behaviour Ecto.Adapters.Worker

  def connect(opts) do
    opts
    |> Enum.map(fn
      {:hostname, hostname} -> {:host, to_erl(hostname)}
      {:username, username} -> {:login, to_erl(username)}
      {:database, database} -> {:database, to_string(database)}
      {key, value} when is_binary(value) -> {key, to_erl(value)}
      other -> other
    end)
    |> :mc_worker.start_link
  end

  def disconnect(conn) do
    :mc_worker.disconnect(conn)
  end

  defp to_erl(nil), do: :undefined
  defp to_erl(string) when is_binary(string), do: to_char_list(string)
  defp to_erl(other), do: other
end
