defmodule Mongo.Ecto.ObjectID do
  @moduledoc """
  An Ecto type to represent MongoDB's ObjectIDs

  Represented as hex-encoded binaries of 24 characters.
  """

  @behaviour Ecto.Type

  @doc """
  The Ecto primitive type.
  """
  def type, do: :binary_id

  @doc """
  Casts to valid hex-encoded binary
  """
  def cast(<<_::24-binary>> = hex), do: {:ok, hex}
  def cast(_), do: :error


  @doc """
  Converts to a format acceptable for the database
  """
  def dump(<< c0,  c1,  c2,  c3,  c4,  c5,
              c6,  c7,  c8,  c9,  c10, c11,
              c12, c13, c14, c15, c16, c17,
              c18, c19, c20, c21, c22, c23 >>) do
    try do
      << d(c0)::4,  d(c1)::4,  d(c2)::4,  d(c3)::4,
         d(c4)::4,  d(c5)::4,  d(c6)::4,  d(c7)::4,
         d(c8)::4,  d(c9)::4,  d(c10)::4, d(c11)::4,
         d(c12)::4, d(c13)::4, d(c14)::4, d(c15)::4,
         d(c16)::4, d(c17)::4, d(c18)::4, d(c19)::4,
         d(c20)::4, d(c21)::4, d(c22)::4, d(c23)::4 >>
    catch
      :throw, :error ->
        :error
    else
      value ->
        {:ok, %Ecto.Query.Tagged{type: :binary_id, value: value}}
    end
  end

  @doc """
  Converts from the format returned from the database
  """
  def load(binary) do
    try do
      {:ok, encode(binary)}
    catch
      :throw, :error ->
        :error
    end
  end

  @doc """
  Generates a new ObjectID
  """
  def generate do
    %BSON.ObjectId{value: value} = Mongo.IdServer.new
    encode(value)
  end

  def encode(<< l0::4, h0::4, l1::4, h1::4,  l2::4,  h2::4,  l3::4,  h3::4,
                l4::4, h4::4, l5::4, h5::4,  l6::4,  h6::4,  l7::4,  h7::4,
                l8::4, h8::4, l9::4, h9::4, l10::4, h10::4, l11::4, h11::4 >>) do
    << e(l0), e(h0), e(l1), e(h1), e(l2),  e(h2),  e(l3),  e(h3),
       e(l4), e(h4), e(l5), e(h5), e(l6),  e(h6),  e(l7),  e(h7),
       e(l8), e(h8), e(l9), e(h9), e(l10), e(h10), e(l11), e(h11) >>
  end

  @compile {:inline, :d, 1}
  @compile {:inline, :e, 1}

  defp d(?0), do: 0
  defp d(?1), do: 1
  defp d(?2), do: 2
  defp d(?3), do: 3
  defp d(?4), do: 4
  defp d(?5), do: 5
  defp d(?6), do: 6
  defp d(?7), do: 7
  defp d(?8), do: 8
  defp d(?9), do: 9
  defp d(?a), do: 10
  defp d(?b), do: 11
  defp d(?c), do: 12
  defp d(?d), do: 13
  defp d(?e), do: 14
  defp d(?f), do: 15
  defp d(?A), do: 10
  defp d(?B), do: 11
  defp d(?C), do: 12
  defp d(?D), do: 13
  defp d(?E), do: 14
  defp d(?F), do: 15
  defp d(_),  do: throw :error

  defp e(0),  do: ?0
  defp e(1),  do: ?1
  defp e(2),  do: ?2
  defp e(3),  do: ?3
  defp e(4),  do: ?4
  defp e(5),  do: ?5
  defp e(6),  do: ?6
  defp e(7),  do: ?7
  defp e(8),  do: ?8
  defp e(9),  do: ?9
  defp e(10), do: ?a
  defp e(11), do: ?b
  defp e(12), do: ?c
  defp e(13), do: ?d
  defp e(14), do: ?e
  defp e(15), do: ?f
  defp e(_),  do: throw :error
end
