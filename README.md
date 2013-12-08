yodb
====

A lightweight and efficient key-value storage engine based on the buffer tree.

## Purpose
There have already been several KV databases like [Tokudb](https://github.com/Tokutek/ft-index) and  [cascadb](https://github.com/weicao/cascadb), which are using buffer tree as the underlying data structure to optimize write operation. But their code seems to be a little complex and hard for the __beginners__ to understand the core idea of how the tree is really works in multiple threading environment.

So I write this storage engine which named __yodb__ (__yo__ just helps for funny pronunciation), try to meet the need of those guys who want to have a quick introspect of this beautiful algorithm. Yodb has an excellent performance that can handle millions of read/write requests at a time with only 6K source lines of code, and also of course has a detailed notation.

## Performance
```

```

## Enjoy
```

```
