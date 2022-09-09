![](https://content.gnoss.ws/imagenes/proyectos/personalizacion/7e72bf14-28b9-4beb-82f8-e32a3b49d9d3/cms/logognossazulprincipal.png)

# Gnoss.BackgroundTask.SearchGraphGeneration.OpenCORE

![](https://github.com/equipognoss/Gnoss.BackgroundTask.SearchGraphGeneration.OpenCORE/workflows/BuildSearchGraphGeneration/badge.svg)

Aplicación de segundo plano que se encarga de insertar en el grafo de búsqueda los triples de cada elemento que se cree en la comunidad (recurso, persona, etc).

Colas que escucha este servicio: 
* ColaTagsComunidades: Se envía un mensaje cada vez que se crea, comparte, edita o elimina un recurso desde la Web o el API para que este servicio cree, modifique o elimine los triples del recurso en una comunidad. 
* ColaTagsCom_Per_Org: Se envía un mensaje cada vez que se registra un usuario, edita su perfil o da de baja de una comunidad desde la Web o el API para que este servicio cree, modifique o elimine los triples de esa persona en una comunidad. 
* ColaTagsProyectos: Se envía un mensaje cada vez que se da de alta, se modifica o se cierra un proyecto desde la Web o el API para que este servicio cree, modifique o elimine los triples de ese proyecto en una comunidad. 
* ColaTagsPaginaCMS: Se envía un mensaje cada vez que se da de alta, se modifica o se elimina una página del CMS a través de la Web para que este servicio cree, modifique o elimine los triples de esa página de CMS en una comunidad. 

Configuración estandar de esta aplicación en el archivo docker-compose.yml: 

```yml
searchgraphgeneration:
    image: gnoss/searchgraphgeneration
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
     scopeIdentity: ${scopeIdentity}
     clientIDIdentity: ${clientIDIdentity}
     clientSecretIdentity: ${clientIDIdentity}
    volumes:
     - ./logs/searchgraphgeneration:/app/logs
```

Se pueden consultar los posibles valores de configuración de cada parámetro aquí: https://github.com/equipognoss/Gnoss.SemanticAIPlatform.OpenCORE

## Código de conducta
Este proyecto a adoptado el código de conducta definido por "Contributor Covenant" para definir el comportamiento esperado en las contribuciones a este proyecto. Para más información ver https://www.contributor-covenant.org/

## Licencia
Este producto es parte de la plataforma [Gnoss Semantic AI Platform Open Core](https://github.com/equipognoss/Gnoss.SemanticAIPlatform.OpenCORE), es un producto open source y está licenciado bajo GPLv3.
