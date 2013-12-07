yodb
====

A lightweight and efficiency key-value storage engine based on the buffer tree.

## Purpose
There have already been some databases like [Tokudb](https://github.com/Tokutek/ft-index) and  [cascadb](https://github.com/weicao/cascadb), which are using buffer tree as the underlying data structure to optimize write operation. But their code seems to be a little complex and hard for the __beginners__ to understand the core idea of how the tree is really works in multiple threading environment.

So I wrote this storage engine which named __yodb__ (__yo__ just helps for funny pronunciation), trying satisfy the need of those guys who want to have a quick introspect of this beautiful algorithm. It has only 6K source lines of code, excellent read/write performance, and also has detailed notation.

## Performance


## Enjoy
