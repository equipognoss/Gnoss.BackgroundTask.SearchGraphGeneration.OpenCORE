![](https://content.gnoss.ws/imagenes/proyectos/personalizacion/7e72bf14-28b9-4beb-82f8-e32a3b49d9d3/cms/logognossazulprincipal.png)

# Gnoss.BackgroundTask.SearchGraphGeneration.OpenCORE

Aplicación de segundo plano que se encarga de insertar en el grafo de búsqueda los triples de cada elemento que se cree en la comunidad (recurso, persona, etc).

Configuración estandar de esta aplicación en el archivo docker-compose.yml: 

```yml
searchgraphgeneration:
    image: searchgraphgeneration
    env_file: .env
    environment:
     virtuosoConnectionString: ${virtuosoConnectionString}
     acid: ${acid}
     base: ${base}
     RabbitMQ__colaReplicacion: ${RabbitMQ}
     RabbitMQ__colaServiciosWin: ${RabbitMQ}
     Virtuoso__Escritura__VirtuosoLecturaPruebasGnoss_v3: "HOST=192.168.2.5:1111;UID=dba;PWD=dba;Pooling=true;Max Pool Size=10;Connection Lifetime=15000"
     Virtuoso__Escritura__VirtuosoLecturaPruebasGnoss_v4: "HOST=192.168.2.6:1111;UID=dba;PWD=dba;Pooling=true;Max Pool Size=10;Connection Lifetime=15000"
     BidirectionalReplication__VirtuosoLecturaPruebasGnoss_v3: "VirtuosoLecturaPruebasGnoss_v4"
     BidirectionalReplication__VirtuosoLecturaPruebasGnoss_v4: "VirtuosoLecturaPruebasGnoss_v3"
     redis__redis__ip__master: ${redis__redis__ip__master}
     redis__redis__bd: ${redis__redis__bd}
     redis__redis__timeout: ${redis__redis__timeout}
     redis__recursos__ip__master: ${redis__recursos__ip__master}
     redis__recursos__bd: ${redis__recursos_bd}
     redis__recursos__timeout: ${redis__recursos_timeout}
     redis__liveUsuarios__ip__master: ${redis__liveUsuarios__ip__master}
     redis__liveUsuarios__bd: ${redis__liveUsuarios_bd}
     redis__liveUsuarios__timeout: ${redis__liveUsuarios_timeout}
     idiomas: "es|Español,en|English"
     Servicios__urlBase: "https://servicios.test.com"
     connectionType: "0"
     intervalo: "100"
    volumes:
     - ./logs/searchgraphgeneration:/app/logs
```

Se pueden consultar los posibles valores de configuración de cada parámetro aquí: https://github.com/equipognoss/Gnoss.Platform.Deploy
