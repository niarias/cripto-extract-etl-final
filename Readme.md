### Proyecto ETL de Criptomonedas con Airflow y Python

Descripción
Este repositorio contiene un proyecto de ETL desarrollado en Airflow y Python, diseñado para extraer, transformar y cargar datos sobre criptomonedas desde diversas fuentes. El proceso ETL se ejecuta de manera automatizada, asegurando que los datos sean actualizados y relevantes.

## Pre-requisitos

Para ejecutar este proyecto, necesitarás tener instalado Docker y Docker Compose en tu sistema. Si no los tienes instalados, puedes seguir las instrucciones en Docker y Docker Compose.

## Instalación y Ejecución

Clonar el repositorio:

```bash
git clone https://github.com/tu_usuario/tu_repositorio.git
cd tu_repositorio
```

## Levantar el proyecto con Docker Compose:

En la raíz del proyecto, encontrarás un archivo docker-compose.yml. Para iniciar el proyecto, ejecuta:

```bash
docker-compose up -d
```

Este comando descargará y construirá las imágenes necesarias, y luego iniciará los contenedores en modo desacoplado.

## Acceso a Airflow:

Una vez que los contenedores estén en ejecución, podrás acceder a la interfaz de Airflow a través de tu navegador en http://localhost:8080.

## Detener y eliminar los contenedores:

Para detener y eliminar los contenedores, utiliza:

```bash
docker-compose down
```

dags/: Contiene los archivos DAG de Airflow que definen las tareas y flujos de trabajo del ETL.
scripts/: Contiene la logica de negocio.
docker-compose.yml: Define los servicios, redes y volúmenes para Docker.
