# Installation

```shell
pip install bundestag
```

or

```shell
uv install bundestag
```

to get access to the cli to download and transform bundestag roll call votes data from bundestag.de or abgeordnetenwatch.de.

By to keep things light the machine learning dependencies are made optional. If you want to get those and related functionality as well run

```shell
pip install bundestag[ml]
```

or

```shell
uv install bundestag[ml]
```

For development

```shell
git clone https://github.com/eschmidt42/bundestag
cd bundestag
make install-dev-env
```
