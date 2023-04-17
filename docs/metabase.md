# Docker

Here some tips if you want to analyze the data using metabase. And a list of commands that may be helpful debugging.

The setup here is based on [metabase's guide](https://www.metabase.com/docs/latest/installation-and-operation/running-metabase-on-docker) and extends it to incorporate a mysql database (for votes/mandates/polls data) and adminer (to verify database accessibility across containers).


## Installing docker

On windows with wsl you may need to get `systemd`: https://devblogs.microsoft.com/commandline/systemd-support-is-now-available-in-wsl/

Official guide to install the docker engine: https://docs.docker.com/engine/install

## The data

Make sure you have `data/preprocessed/abgeordnetenwatch/*111.parquet` available.

For this you need to [set up the repo](installation.md) and then run
```shell
bundestag download huggingface
```
as described in [how-to-use](how-to-use.md).

## Installing & accessing metabase

Run
```shell
sudo docker compose up -d
```

This should build 5 containers: mysql, ingestion, postgres, adminer, metabase. This may take a few minutes.

If everything started correctly, see [List of helpful commands](#List-of-helpful-commands) below for helpful command line arguments to verify, you should be able to access adminer and metabase in your browser under:

* Adminer: `localhost:8888` and
* metabase: `localhost:3000`.

The credentials can be found in `.env` for postgres and mysql respectively.

Happy plotting!

**On your first start up of metabase**:

* you will be asked to create an account (just be creative but also write down the email / password combination you used for later) and
* you will also have to manually add the mysql connection.

## List of helpful commands

List containers
```shell
sudo docker ps -a
```

Stop containers
```shell
sudo docker stop <container_id>
```

Remove containers
```shell
sudo docker rm <container_id>
```

To list / remove volumes used to store data requried for mysql and postgresql:
```shell
sudo docker volume ls
sudo docker volume rm <name>
```

To list / remove images
```shell
sudo docker images -a
sudo docker rmi <image_name>
```
More useful commands like that here:
https://www.digitalocean.com/community/tutorials/how-to-remove-docker-images-containers-and-volumes

Composing a by profile (here the `mysql` container)
```shell
sudo docker compose --profile mysql up -d
```

Inspecting the logs of a container
```shell
sudo docker logs --tail 50 --follow --timestamps
<container_id>
```

Remove dangling images: https://docs.docker.com/config/pruning/
```shell
sudo docker image prune
```

## References

[1] example for ingestion with metabase + mysql ([blog](https://medium.com/@stefentaime_10958/end-to-end-data-engineering-project-with-spark-mongodb-minio-postgres-and-metabase-2c400672b50d), [github](https://github.com/Stefen-Taime/projet_data/tree/main/loader))

[2] example for a data stack ([blog](https://medium.com/@isochoa95/building-a-simple-data-stack-with-docker-aceeff4264b7), [github](https://github.com/iagochoa/DataStack/blob/main/docker-compose.yaml))
