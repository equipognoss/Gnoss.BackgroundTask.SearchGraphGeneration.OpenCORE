using System;
using System.Collections.Generic;
using System.Threading;
using System.Data;
using Es.Riam.Gnoss.AD.BASE_BD.Model;
using Es.Riam.Gnoss.AD.BASE_BD;
using Es.Riam.Gnoss.Logica.BASE_BD;
using Es.Riam.Gnoss.AD.Tags;
using Es.Riam.Gnoss.Logica.ServiciosGenerales;
using Es.Riam.Gnoss.AD.ServiciosGenerales;
using Es.Riam.Gnoss.Logica.Documentacion;
using Es.Riam.Gnoss.Logica.Facetado;
using Es.Riam.Gnoss.AD.Facetado;
using System.Diagnostics;
using Es.Riam.Gnoss.Recursos;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Logica.CMS;
using Es.Riam.Util;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Semantica.OWL;
using Es.Riam.Gnoss.RabbitMQ;
using Newtonsoft.Json;
using Es.Riam.Gnoss.AD.EntityModel.Models.ProyectoDS;
using System.Linq;
using Es.Riam.Gnoss.AD.EncapsuladoDatos;
using Es.Riam.Gnoss.AD.Facetado.Model;
using Es.Riam.Gnoss.Logica.ParametroAplicacion;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.AD.EntityModel.Models.BASE;
using Es.Riam.AbstractsOpen;
using Es.Riam.Gnoss.UtilServiciosWeb;
using System.Text;

namespace GnossServicioModuloBASE
{
    internal class ControladorColaRecursos : Controlador
    {
        #region Constantes

        private const string COLA_TAGS_COMUNIDADES = "ColaTagsComunidades";
        private const string EXCHANGE = "";
        private const string VERIFICAR_ONTOLOGIAS = "VERIFICAR_SERVICIO_ONTOLOGIAS";

        #endregion

        /// <summary>
        /// 
        /// </summary>
        /// <param name="pReplicacion"></param>
        /// <param name="pRutaBaseTriplesDescarga"></param>
        /// <param name="pUrlTriplesDescarga"></param>
        /// <param name="pEmailErrores"></param>
        /// <param name="pHoraEnvioErrores"></param>
        /// <param name="pEscribirFicheroExternoTriples"></param>
        public ControladorColaRecursos(bool pReplicacion, string pRutaBaseTriplesDescarga, string pUrlTriplesDescarga, string pEmailErrores, int pHoraEnvioErrores, bool pEscribirFicheroExternoTriples,IServiceScopeFactory serviceScope, ConfigService configService, int sleep = 0)
            : base(pReplicacion, pRutaBaseTriplesDescarga, pUrlTriplesDescarga, pEmailErrores, pHoraEnvioErrores, pEscribirFicheroExternoTriples, serviceScope, configService, sleep)
        {
        }

        protected override void RealizarMantenimientoRabbitMQ(LoggingService loggingService, bool reintentar = true)
        {
            
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItem);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDown);

                RabbitMQClient rMQ = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, COLA_TAGS_COMUNIDADES,loggingService, mConfigService, EXCHANGE, COLA_TAGS_COMUNIDADES);

                try
                {
                    rMQ.ObtenerElementosDeCola(funcionProcesarItem, funcionShutDown);
                }
                catch (Exception ex)
                {
                    if (reintentar)
                    {
                        //Puede que la cola no este creada, la creamos con un elemento vacio
                        rMQ.AgregarElementoACola("");

                        RealizarMantenimientoRabbitMQ(loggingService, false);
                    }
                    else
                    {
                        loggingService.GuardarLogError(ex);
                        throw;
                    }
                }
            }
        }

        

        public bool ProcesarItem(string pFila)
        {
            using (var scope = ScopedFactory.CreateScope())
            {
                EntityContext entityContext = scope.ServiceProvider.GetRequiredService<EntityContext>();
                entityContext.SetTrackingFalse();
                EntityContextBASE entityContextBASE = scope.ServiceProvider.GetRequiredService<EntityContextBASE>();
                UtilidadesVirtuoso utilidadesVirtuoso = scope.ServiceProvider.GetRequiredService<UtilidadesVirtuoso>();
                LoggingService loggingService = scope.ServiceProvider.GetRequiredService<LoggingService>();
                VirtuosoAD virtuosoAD = scope.ServiceProvider.GetRequiredService<VirtuosoAD>();
                RedisCacheWrapper redisCacheWrapper = scope.ServiceProvider.GetRequiredService<RedisCacheWrapper>();
                GnossCache gnossCache = scope.ServiceProvider.GetRequiredService<GnossCache>();
                ConfigService configService = scope.ServiceProvider.GetRequiredService<ConfigService>();
                IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                ComprobarTraza("SearchGraphGeneration", entityContext, loggingService, redisCacheWrapper, configService, servicesUtilVirtuosoAndReplication);
                bool error = false;
                try
                {
                    ComprobarCancelacionHilo();

                    Debug.WriteLine($"ProcesarItem, {pFila}!");

                    if (!string.IsNullOrEmpty(pFila))
                    {
                        object[] itemArray = JsonConvert.DeserializeObject<object[]>(pFila);
                        BaseRecursosComunidadDS.ColaTagsComunidadesRow filaCola = (BaseRecursosComunidadDS.ColaTagsComunidadesRow)new BaseRecursosComunidadDS().ColaTagsComunidades.Rows.Add(itemArray);
                        itemArray = null;

                        if (filaCola.Tags.Equals(VERIFICAR_ONTOLOGIAS))
                        {
                            VerificarServicioOntologias(gnossCache, configService, loggingService);
                        }
                        else
                        {
                            error = ProcesarFilasDeColaDeRecursos((BaseRecursosComunidadDS)filaCola.Table.DataSet, entityContext, loggingService, entityContextBASE, virtuosoAD, redisCacheWrapper, utilidadesVirtuoso, gnossCache, servicesUtilVirtuosoAndReplication, true);

                            if (!error)
                            {

                                InsertarColaTagsComunidades(filaCola, loggingService);
                            }
                        }
                        filaCola = null;


                        ControladorConexiones.CerrarConexiones(false);
                    }
                }
                catch(Exception ex)
                {
                    GuardarLog(ex, loggingService);
                    return true;
                }
                finally
                {
                    GuardarTraza(loggingService);
                }
                return true;
            }
        }

        private void VerificarServicioOntologias(GnossCache pGnossCache, ConfigService pConfigService, LoggingService pLoggingService)
        {
            string cadenaDePrueba = "ontologia vacia";

            try
            {
                CallFileService servicioArch = new CallFileService(pConfigService, pLoggingService);
                servicioArch.GuardarOntologia(Encoding.UTF8.GetBytes(cadenaDePrueba), ProyectoAD.MetaProyecto);

                string respuesta = servicioArch.ObtenerOntologia(ProyectoAD.MetaProyecto);

                pGnossCache.AgregarObjetoCache(VERIFICAR_ONTOLOGIAS, respuesta.Equals(cadenaDePrueba), 60);
            }
            catch(Exception ex)
            {
                mLoggingService.GuardarLogError(ex);
                pGnossCache.AgregarObjetoCache(VERIFICAR_ONTOLOGIAS, false, 60);
            }
        }

        /// <summary>
        /// Inserta en la cola de Rabbit los tags de las comunidades
        /// </summary>
        /// <param name="pFilaCola">Parámetro de la fila para la cola</param>
        public void InsertarColaTagsComunidades(BaseRecursosComunidadDS.ColaTagsComunidadesRow pFilaCola, LoggingService loggingService)
        {
            string exchange = "";
            string colaRabbit = "ColaTagsComunidadesGeneradorAutocompletar";
            
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient rabbitMQ = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, colaRabbit, loggingService, mConfigService, exchange, colaRabbit);
                rabbitMQ.AgregarElementoACola(JsonConvert.SerializeObject(pFilaCola.ItemArray));
                rabbitMQ.Dispose();
            }
        }

        /// <summary>
        /// Procesa las filas de recursos
        /// </summary>
        /// <returns>Verdad si ha habido algún error</returns>
        private bool ProcesarFilasDeColaDeRecursos(BaseRecursosComunidadDS pBaseRecursosComunidadDS, EntityContext entityContext, LoggingService loggingService, EntityContextBASE entityContextBASE, VirtuosoAD virtuosoAD, RedisCacheWrapper redisCacheWrapper, UtilidadesVirtuoso utilidadesVirtuoso, GnossCache gnossCache, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication, bool pEsFilaRabbit = false)
        {
            ParametroAplicacionCN paramApp = new ParametroAplicacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            string valor = paramApp.ObtenerParametroBusquedaPorTextoLibrePersonalizado();
            bool noGenerarSearch = !string.IsNullOrEmpty(valor) && valor.Equals("1");
            bool error = false;

            //Rellenamos este diccionario con los recursos agregados de una misma comunidad para procesarolos de una vez
            Dictionary<int, List<BaseRecursosComunidadDS.ColaTagsComunidadesRow>> listaRecursosAgregadosMasivosPorProyecto = new Dictionary<int, List<BaseRecursosComunidadDS.ColaTagsComunidadesRow>>();
            foreach (BaseRecursosComunidadDS.ColaTagsComunidadesRow filaCola in pBaseRecursosComunidadDS.ColaTagsComunidades.Rows)
            {
                if (!filaCola.Tags.Contains(Constantes.ID_PROY_DESTINO) && filaCola.Tipo == (short)TiposElementosEnCola.Agregado && filaCola.Estado == 0)
                {
                    if (listaRecursosAgregadosMasivosPorProyecto.ContainsKey(filaCola.TablaBaseProyectoID))
                    {
                        listaRecursosAgregadosMasivosPorProyecto[filaCola.TablaBaseProyectoID].Add(filaCola);
                    }
                    else
                    {
                        List<BaseRecursosComunidadDS.ColaTagsComunidadesRow> listaFilas = new List<BaseRecursosComunidadDS.ColaTagsComunidadesRow>();
                        listaFilas.Add(filaCola);
                        listaRecursosAgregadosMasivosPorProyecto.Add(filaCola.TablaBaseProyectoID, listaFilas);
                    }
                }

                
            }

            //Realizamos las cargas
            //ID de recursos
            List<Guid> listasRecursosID = new List<Guid>();
            //Filas de proyecto
            Dictionary<int, Proyecto> filasProyecto = new Dictionary<int, Proyecto>();
            //Tiene componente con caducidad proyceto
            Dictionary<int, bool> ComponenteConCaducidadTipoRecursoProyecto = new Dictionary<int, bool>();
            //Tags BBDD
            Dictionary<Guid, List<string>> listaTagsBBDD = new Dictionary<Guid, List<string>>();
            //Titulos recursos BBDD
            Dictionary<Guid, string> titulosRecursosBBDD = new Dictionary<Guid, string>();
            //Descripcion recursos BBDD
            Dictionary<Guid, string> descripcionRecursosBBDD = new Dictionary<Guid, string>();
            //Tags documento            
            Dictionary<Guid, List<string>> listaTagsDirectos = new Dictionary<Guid, List<string>>();
            Dictionary<Guid, List<string>> listaTagsIndirectos = new Dictionary<Guid, List<string>>();
            Dictionary<Guid, Dictionary<short, List<string>>> listaTagsFiltros = new Dictionary<Guid, Dictionary<short, List<string>>>();
            Dictionary<Guid, List<string>> listaTodosTags = new Dictionary<Guid, List<string>>();
            Dictionary<int, Guid> listaOrdenEjecucionDoc = new Dictionary<int, Guid>();
            //Cargas de virtuoso
            DataWrapperFacetas tConfiguracion = new DataWrapperFacetas();
            Dictionary<Guid, bool> listaDocsBorradores = new Dictionary<Guid, bool>();

            //Diccionario con clave proyecto y valor ( clave documento valor DATASET de FACETADS obtenido por ObtieneInformacionComunRecurso )
            Dictionary<Guid, Dictionary<Guid, FacetaDS>> diccionarioProyectoDocInformacionComunRecurso = new Dictionary<Guid, Dictionary<Guid, FacetaDS>>();
            //Diccionario con clave proyecto y valor ( clave documento valor DATASET de FACETADS obtenido por ObtieneInformacionExtraRecurso )
            Dictionary<Guid, Dictionary<Guid, FacetaDS>> diccionarioProyectoDocInformacionExtraRecurso = new Dictionary<Guid, Dictionary<Guid, FacetaDS>>();

            ProyectoCN proyectoCN = new ProyectoCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            ActualizacionFacetadoCN actualizacionFacetadoCN = new ActualizacionFacetadoCN(mFicheroConfiguracionBD, mUrlIntragnoss, entityContext, loggingService, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
            Guid proyectoID = Guid.Empty;
            foreach (int claveProyecto in listaRecursosAgregadosMasivosPorProyecto.Keys)
            {
                //Solo hacemos las cargas masivas si hay mas de un recurso
                if (listaRecursosAgregadosMasivosPorProyecto[claveProyecto].Count > 1)
                {
                    try
                    {
                        //FilaProyecto
                        Proyecto filaProyecto = proyectoCN.ObtenerProyectoPorTablaBaseProyectoID(claveProyecto).ListaProyecto.FirstOrDefault();
                        filasProyecto.Add(claveProyecto, filaProyecto);
                        proyectoID = filaProyecto.ProyectoID;
                        //TieneComponenteCMS
                        CMSCN cmsCN = new CMSCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                        bool TieneComponenteConCaducidadTipoRecurso = cmsCN.ObtenerSiTieneComponenteConCaducidadTipoRecurso(filaProyecto.ProyectoID);
                        cmsCN.Dispose();
                        ComponenteConCaducidadTipoRecursoProyecto.Add(claveProyecto, TieneComponenteConCaducidadTipoRecurso);

                        List<Guid> listaDocsProyecto = new List<Guid>();

                        //Realizamos la carga por Documento
                        foreach (BaseRecursosComunidadDS.ColaTagsComunidadesRow filaCola in listaRecursosAgregadosMasivosPorProyecto[claveProyecto])
                        {
                            List<string> listaTagsDirectosInt = new List<string>();
                            List<string> listaTagsIndirectosInt = new List<string>();
                            Dictionary<short, List<string>> listaTagsFiltrosInt = new Dictionary<short, List<string>>();
                            List<string> listaTodosTagsInt = SepararTags((string)filaCola["Tags"], listaTagsDirectosInt, listaTagsIndirectosInt, listaTagsFiltrosInt, filaCola.Table.DataSet);
                            Guid idRecurso = new Guid(listaTagsFiltrosInt[(short)TiposTags.IDTagDoc][0]);

                            if (!listaDocsProyecto.Contains(idRecurso))
                            {
                                listaDocsProyecto.Add(idRecurso);
                            }

                            string comentarioorecurso = " ";
                            if (listaTagsFiltrosInt[(short)TiposTags.ComentarioORecurso].Count > 0)
                            {
                                comentarioorecurso = listaTagsFiltrosInt[(short)TiposTags.ComentarioORecurso][0];
                            }

                            if (comentarioorecurso.Contains("c"))
                            {
                                //TODO Obtener de virtuoso
                            }
                            else if (comentarioorecurso.Contains("f"))
                            {
                                //TODO Obtener de virtuoso
                            }
                            else
                            {
                                listaOrdenEjecucionDoc.Add(filaCola.OrdenEjecucion, idRecurso);
                                if (!listasRecursosID.Contains(idRecurso))
                                {
                                    listasRecursosID.Add(idRecurso);
                                }
                            }

                            if (!listaTagsDirectos.ContainsKey(idRecurso))
                            {
                                listaTagsDirectos.Add(idRecurso, listaTagsDirectosInt);
                            }
                            if (!listaTagsIndirectos.ContainsKey(idRecurso))
                            {
                                listaTagsIndirectos.Add(idRecurso, listaTagsDirectosInt);
                            }
                            if (!listaTagsFiltros.ContainsKey(idRecurso))
                            {
                                listaTagsFiltros.Add(idRecurso, listaTagsFiltrosInt);
                            }
                            if (!listaTodosTags.ContainsKey(idRecurso))
                            {
                                listaTodosTags.Add(idRecurso, listaTodosTagsInt);
                            }

                        }
                        List<QueryTriples> listaResultadosInformacionComunRecurso = new List<QueryTriples>();
                        if (!diccionarioProyectoDocInformacionComunRecurso.ContainsKey(filaProyecto.ProyectoID))
                        {
                            //ObtieneInformacionComunRecurso por proyecto
                            Dictionary<Guid, FacetaDS> facetas = new Dictionary<Guid, FacetaDS>();
                            listaResultadosInformacionComunRecurso.AddRange(actualizacionFacetadoCN.ObtieneInformacionComunRecurso(listaDocsProyecto, filaProyecto.ProyectoID));
                            diccionarioProyectoDocInformacionComunRecurso.Add(filaProyecto.ProyectoID, facetas);
                        }

                        List<QueryTriples> listaResultadosInformacionExtraRecurso = new List<QueryTriples>();
                        if (!diccionarioProyectoDocInformacionExtraRecurso.ContainsKey(filaProyecto.ProyectoID))
                        {
                            //ObtieneInformacionExtraRecurso por proyecto
                            Dictionary<Guid, FacetaDS> facetas = new Dictionary<Guid, FacetaDS>();
                            listaResultadosInformacionExtraRecurso.AddRange(actualizacionFacetadoCN.ObtieneInformacionExtraRecurso(listaDocsProyecto, filaProyecto.ProyectoID));
                            diccionarioProyectoDocInformacionExtraRecurso.Add(filaProyecto.ProyectoID, facetas);
                        }
                    }
                    catch (Exception ex)
                    {
                        loggingService.GuardarLogError("ERROR IMPORTANTE CARGA MULTIPLE:  " + ex.ToString());
                    }
                }
            }
            proyectoCN.Dispose();

            if (listasRecursosID.Count > 0)
            {
                try
                {
                    //Si hay multiples documentos hacemos cargas multiples
                    DocumentacionCN documentacionCN = new DocumentacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                    FacetaDS facetaDS = new FacetaDS();
                    actualizacionFacetadoCN.ObtieneInformacionExtraRecursosContribuciones(listasRecursosID, Guid.Empty, proyectoID);

                    //Cargamos si son borradores
                    
                    listaDocsBorradores = documentacionCN.EsDocumentoBorradorLista(listasRecursosID);
                    documentacionCN.Dispose();

                    //Cargamos los tags de BBDD (en el caso de los recursos el proyectoID es irrelevante)
                    listaTagsBBDD = actualizacionFacetadoCN.ObtenerTagsLista(listasRecursosID, "Recurso", Guid.Empty);

                    //Cargamos los titulos de los recursos
                    titulosRecursosBBDD = actualizacionFacetadoCN.ObtenerTitulosRecursos(listasRecursosID);

                    //Cargamos las descripciones de los recursos
                    descripcionRecursosBBDD = actualizacionFacetadoCN.ObtenerDescripcionesRecursos(listasRecursosID);
                    List<QueryTriples> listaResultadosInformacionComunRecurso = new List<QueryTriples>();
                    if (!diccionarioProyectoDocInformacionComunRecurso.ContainsKey(ProyectoAD.MetaProyecto))
                    {
                        //ObtieneInformacionComunRecurso por proyecto (MYGNOSS)
                        Dictionary<Guid, FacetaDS> facetas = new Dictionary<Guid, FacetaDS>();
                        listaResultadosInformacionComunRecurso.AddRange(actualizacionFacetadoCN.ObtieneInformacionComunRecurso(listasRecursosID, ProyectoAD.MetaProyecto));
                        diccionarioProyectoDocInformacionComunRecurso.Add(ProyectoAD.MetaProyecto, facetas);
                    }
                    List<QueryTriples> listaResultadosInformacionExtraRecurso = new List<QueryTriples>();
                    if (!diccionarioProyectoDocInformacionExtraRecurso.ContainsKey(ProyectoAD.MetaProyecto))
                    {
                        //ObtieneInformacionExtraRecurso por proyecto (MYGNOSS)
                        Dictionary<Guid, FacetaDS> facetas = new Dictionary<Guid, FacetaDS>();
                        listaResultadosInformacionExtraRecurso.AddRange(actualizacionFacetadoCN.ObtieneInformacionExtraRecurso(listasRecursosID, ProyectoAD.MetaProyecto));
                        diccionarioProyectoDocInformacionExtraRecurso.Add(ProyectoAD.MetaProyecto, facetas);
                    }
                }
                catch (Exception ex)
                {
                    loggingService.GuardarLogError("ERROR IMPORTANTE CARGA MULTIPLE:  " + ex.ToString());
                }
            }
            actualizacionFacetadoCN.Dispose();

            //Recorro las filas de comunidades
            foreach (BaseRecursosComunidadDS.ColaTagsComunidadesRow filaCola in pBaseRecursosComunidadDS.ColaTagsComunidades.Rows)
            {
                if (filaCola.Tags.Contains(Constantes.ID_PROY_DESTINO))
                {
                    error = error || ProcesarFilaCompartirOntologia(filaCola, entityContext, loggingService, virtuosoAD, utilidadesVirtuoso, entityContextBASE, redisCacheWrapper, gnossCache, servicesUtilVirtuosoAndReplication);
                }
                else
                {
                    //Compruebo si la tabla está creada en la BD
                    if (filasProyecto.ContainsKey(filaCola.TablaBaseProyectoID) && listaOrdenEjecucionDoc.ContainsKey(filaCola.OrdenEjecucion))
                    {
                        error = error || ProcesarFilaDeCola(filaCola, filasProyecto[filaCola.TablaBaseProyectoID], ComponenteConCaducidadTipoRecursoProyecto[filaCola.TablaBaseProyectoID], listaTagsDirectos[listaOrdenEjecucionDoc[filaCola.OrdenEjecucion]], listaTagsIndirectos[listaOrdenEjecucionDoc[filaCola.OrdenEjecucion]], listaTagsFiltros[listaOrdenEjecucionDoc[filaCola.OrdenEjecucion]], listaTodosTags[listaOrdenEjecucionDoc[filaCola.OrdenEjecucion]], tConfiguracion, listaDocsBorradores, listaTagsBBDD[listaOrdenEjecucionDoc[filaCola.OrdenEjecucion]], titulosRecursosBBDD[listaOrdenEjecucionDoc[filaCola.OrdenEjecucion]], descripcionRecursosBBDD[listaOrdenEjecucionDoc[filaCola.OrdenEjecucion]], diccionarioProyectoDocInformacionComunRecurso, entityContext, loggingService, virtuosoAD, entityContextBASE, redisCacheWrapper, utilidadesVirtuoso, gnossCache, servicesUtilVirtuosoAndReplication);
                    }
                    else
                    {
                        error = error || ProcesarFilaDeCola(filaCola, entityContext, loggingService, virtuosoAD, entityContextBASE, redisCacheWrapper, utilidadesVirtuoso, gnossCache, servicesUtilVirtuosoAndReplication);
                    }
                }

                if (!pEsFilaRabbit)
                {
                    ControladorConexiones.CerrarConexiones(false);
                    ComprobarCancelacionHilo();
                }

                EstaHiloActivo = true;
            }

            if (pBaseRecursosComunidadDS != null)
            {
                pBaseRecursosComunidadDS.Dispose();
                pBaseRecursosComunidadDS = null;
            }

            return error;
        }

        /// <summary>
        /// Procesa una fila de la cola para compartición de ontología recursos
        /// </summary>
        /// <param name="pFila">Fila de cola a procesar</param>
        /// <returns>Verdad si ha habido algun error durante la operación</returns>
        private bool ProcesarFilaCompartirOntologia(DataRow pFila, EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, UtilidadesVirtuoso utilidadesVirtuoso, EntityContextBASE entityContextBASE, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            bool error = false;
            DataSet brComunidadActualDS = null;

            try
            {
                short estado = (short)pFila["Estado"];
                if (estado < 2)
                {
                    string tags = (string)pFila["Tags"];

                    //id del documento a compartir
                    int inicioGuid = Constantes.ID_TAG_DOCUMENTO.ToString().Length;
                    int finGuid = tags.LastIndexOf(Constantes.ID_TAG_DOCUMENTO) - Constantes.ID_TAG_DOCUMENTO.Length;

                    Guid documentoCompartidoID = new Guid(tags.Substring(inicioGuid, finGuid));

                    //nos quedamos con los id's de los proyectos de destino
                    string proyectosDestino = tags.Substring(tags.IndexOf(Constantes.ID_PROY_DESTINO));
                    proyectosDestino = proyectosDestino.Replace(Constantes.ID_PROY_DESTINO, "");
                    List<Guid> listaID = new List<Guid>();
                    string[] array = proyectosDestino.Split(new char[] { ',' }, StringSplitOptions.RemoveEmptyEntries);

                    foreach (string id in array)
                    {
                        listaID.Add(new Guid(id));
                    }

                    //obtener el documento a partir del id
                    DocumentacionCN docCN = new DocumentacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                    DataWrapperDocumentacion docDW = docCN.ObtenerDocumentoPorID(documentoCompartidoID);
                    string enlaceOnto = docCN.ObtenerEnlaceDocumentoVinculadoADocumento(documentoCompartidoID);
                    docCN.Dispose();
                    string enlace = string.Empty;
                    List<Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.Documento> filaDoc = docDW.ListaDocumento.Where(doc => doc.DocumentoID.Equals(documentoCompartidoID)).ToList();

                    if (filaDoc.Count > 0)
                    {
                        enlace = filaDoc[0].Enlace;
                    }

                    //proyecto desde el que se comparte
                    ProyectoCN proyectoCN = new ProyectoCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                    Proyecto filaProyecto = proyectoCN.ObtenerProyectoPorTablaBaseProyectoID((int)pFila["TablaBaseProyectoID"]).ListaProyecto.FirstOrDefault();
                    Guid proyectoID = filaProyecto.ProyectoID;
                    Guid organizacionID = filaProyecto.OrganizacionID;
                    proyectoCN.Dispose();

                    //coger enlace y llamar a una funcion de virtuoso para obtener las triples de esa ontologia a copiar
                    FacetadoCN facCN = new FacetadoCN(mFicheroConfiguracionBD, mUrlIntragnoss, null, entityContext, loggingService, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    FacetadoDS facDS = facCN.ObtenerTriplesOntologiaACompartir(proyectoID, enlace);
                    facCN.Dispose();

                    string triples = GenerarTripletasCompartirOntologia(documentoCompartidoID, organizacionID, proyectoID, enlace, facDS.Tables["CompartirOntologia"], loggingService, entityContext, redisCacheWrapper, gnossCache, entityContextBASE, virtuosoAD, servicesUtilVirtuosoAndReplication);

                    //actualizo virtuoso, se insertan las triples en cada proyecto en el que se deba compartir
                    foreach (Guid id in listaID)
                    {
                        //UpdateVirtuoso(id.ToString(), triples);

                        InsertarTripletas_ControlCheckPoint(ObtenerPrioridadFila(pFila), id.ToString(), triples, 0, entityContext, loggingService, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                    }

                    pFila["Estado"] = (short)EstadosColaTags.Procesado;

                    if (enlaceOnto != null)
                    {
                        FacetaCN tablasDeConfiguracionCN = new FacetaCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                        Es.Riam.Gnoss.AD.EntityModel.Models.Faceta.OntologiaProyecto filaOntologia = tablasDeConfiguracionCN.ObtenerOntologiaProyectoPorEnlace(proyectoID, enlaceOnto);
                        if (filaOntologia != null && !filaOntologia.EsBuscable)
                        {
                            pFila["EstadoTags"] = (short)EstadosColaTags.Procesado;
                        }
                        facCN.Dispose();
                    }
                }
            }
            catch (Exception exFila)
            {
                //Ha habido algún error durante la operación, notifico el error
                error = true;

                string mensaje = "Excepción: " + exFila.ToString() + "\n\n\tTraza: " + exFila.StackTrace + "\n\nFila: " + pFila["OrdenEjecucion"];
                loggingService.GuardarLogError("ERROR:  " + mensaje);

                pFila["Estado"] = ((short)pFila["Estado"]) + 1; //Aumento en 1 el error, cuando llegue a 4 no se volverá a intentar

                // Se envía al visor de sucesos una notificación
                try
                {
                    string sSource;
                    string sLog;
                    string sEvent;

                    sSource = "Servicio_Modulo_BASE";
                    sLog = "Servicios_GNOSS";
                    sEvent = mensaje;

                    if (!EventLog.SourceExists(sSource))
                        EventLog.CreateEventSource(sSource, sLog);

                    EventLog.WriteEntry(sSource, sEvent, EventLogEntryType.Warning, 888);
                }
                catch (Exception) { }
            }
            finally
            {
                if (brComunidadActualDS != null)
                {
                    brComunidadActualDS.Dispose();
                    brComunidadActualDS = null;
                }

                pFila["FechaProcesado"] = DateTime.Now;

                ControladorConexiones.CerrarConexiones(false);
            }

            return error;
        }

        private string GenerarTripletasCompartirOntologia(Guid pDocumentoID, Guid pOrganizacionID, Guid pProyectoID, string pEnlace, DataTable pTabla, LoggingService loggingService, EntityContext entityContext, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, EntityContextBASE entityContextBASE, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            List<string> Fecha = new List<string>();
            List<string> Numero = new List<string>();
            List<string> TextoInvariable = new List<string>();

            Ontologia ontologia = CargarOntologia(pDocumentoID, pProyectoID, loggingService, entityContext, redisCacheWrapper, gnossCache, entityContextBASE, virtuosoAD, servicesUtilVirtuosoAndReplication);

            UtilidadesVirtuoso.ObtenerListaTipoElementosOntologia(mFicheroConfiguracionBD, pDocumentoID, pProyectoID, ontologia, DicPropiedadesOntologia, ref Fecha, ref Numero);

            ObtenerListasTipoElementosFacetas(pProyectoID, pOrganizacionID, ref Fecha, ref Numero, ref TextoInvariable, entityContext, loggingService, servicesUtilVirtuosoAndReplication);


            string triples = string.Empty;
            FacetadoAD facetadoAD = new FacetadoAD(mFicheroConfiguracionBD, mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);

            foreach (DataRow myrow in pTabla.Rows)
            {
                try
                {
                    if (!((string)myrow[1]).Contains("http://www.w3.org/1999/02/22-rdf-syntax-ns#type") && !((string)myrow[1]).Contains("http://www.w3.org/2000/01/rdf-schema#label"))
                    {
                        string objeto = (string)myrow[2];
                        objeto = objeto.Replace("\r\n", "");
                        objeto = objeto.Replace("\n", "");

                        if (objeto[0] == '"')
                        {
                            objeto = objeto.Substring(1);
                        }

                        if (objeto[objeto.Length - 1] == '"')
                        {
                            objeto = objeto.Substring(0, objeto.Length - 1);
                        }

                        if (objeto[0] == '<')
                        {
                            objeto = objeto.Substring(1);
                        }

                        if (objeto[objeto.Length - 1] == '>')
                        {
                            objeto = objeto.Substring(0, objeto.Length - 1);
                        }

                        string predicado = (string)myrow[1];
                        //predicado = predicado.Substring(1, predicado.Length - 2);                        

                        string sujeto = (string)myrow[0];
                        if (objeto.Contains("/") && (predicado.Contains("fecha") || predicado.Contains("date")))
                        {
                            objeto = ConvertirFormatoFecha(objeto) + " . ";
                        }

                        string idioma = null;

                        if (!myrow.IsNull("idioma") && !string.IsNullOrEmpty((string)myrow["idioma"]))
                        {
                            idioma = (string)myrow["idioma"];
                        }

                        triples += facetadoAD.GenerarTripletaRecogidadeVirtuoso(sujeto, predicado, objeto, objeto, Fecha, Numero, TextoInvariable, null, idioma);
                    }
                }
                catch (Exception) { }
            }

            facetadoAD.Dispose();

            return triples;

        }

        /// <summary>
        /// Coge una fecha en formato 01/01/2010 y lo cambia a 20100101000000
        /// </summary>
        /// <returns>Fecha Cambiada</returns>
        private string ConvertirFormatoFecha(string fecha)
        {
            fecha = fecha.Trim();

            string nfecha;

            if (fecha.IndexOf("/").Equals(2))
            {
                nfecha = fecha.Substring(fecha.LastIndexOf("/") + 1, 4);
                fecha = fecha.Substring(0, fecha.LastIndexOf("/"));
                nfecha += fecha.Substring(fecha.LastIndexOf("/") + 1, 2);
                fecha = fecha.Substring(0, fecha.LastIndexOf("/"));
                nfecha += fecha.Substring(0, 2);
            }
            else
            {
                nfecha = fecha.Substring(0, fecha.IndexOf("/"));
                fecha = fecha.Substring(fecha.IndexOf("/") + 1);
                nfecha += fecha.Substring(0, fecha.IndexOf("/"));
                fecha = fecha.Substring(fecha.IndexOf("/") + 1);
                nfecha += fecha;
            }


            return nfecha + "000000";

        }


        protected override ControladorServicioGnoss ClonarControlador()
        {
            ControladorColaRecursos controlador = new ControladorColaRecursos(mReplicacion, mRutaBaseTriplesDescarga, mUrlTriplesDescarga, mEmailErrores, mHoraEnvioErrores, mEscribirFicheroExternoTriples, ScopedFactory, mConfigService);
            return controlador;
        }
    }
}
