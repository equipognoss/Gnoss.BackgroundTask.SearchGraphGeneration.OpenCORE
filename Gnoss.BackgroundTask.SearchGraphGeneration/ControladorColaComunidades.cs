using System;
using System.Threading;
using Es.Riam.Gnoss.AD.BASE_BD.Model;
using Es.Riam.Gnoss.Logica.BASE_BD;
using System.Diagnostics;
using Es.Riam.Gnoss.Recursos;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Util;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Gnoss.RabbitMQ;
using Newtonsoft.Json;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.AD.EntityModel;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.AD.Virtuoso;
using Es.Riam.Gnoss.CL;
using Microsoft.EntityFrameworkCore;
using Microsoft.EntityFrameworkCore.Storage;
using Es.Riam.AbstractsOpen;

namespace GnossServicioModuloBASE
{
    internal class ControladorColaComunidades : Controlador
    {
        /// <summary>
        /// 
        /// </summary>
        /// <param name="pFicheroConfiguracionBD"></param>
        /// <param name="pReplicacion"></param>
        /// <param name="pRutaBaseTriplesDescarga"></param>
        /// <param name="pUrlTriplesDescarga"></param>
        /// <param name="pEmailErrores"></param>
        /// <param name="pHoraEnvioErrores"></param>
        /// <param name="pEscribirFicheroExternoTriples"></param>
        public ControladorColaComunidades(bool pReplicacion, string pRutaBaseTriplesDescarga, string pUrlTriplesDescarga, string pEmailErrores, int pHoraEnvioErrores, bool pEscribirFicheroExternoTriples, IServiceScopeFactory serviceScope, ConfigService configService, int sleep = 0)
            : base(pReplicacion, pRutaBaseTriplesDescarga, pUrlTriplesDescarga, pEmailErrores, pHoraEnvioErrores, pEscribirFicheroExternoTriples, serviceScope, configService)
        {
        }

        /// <summary>
        /// Carga los mantenimientos pendientes
        /// </summary>
        /// <returns>Verdad si hay algún elemento que procesar</returns>
        protected override bool CargarDatos(EntityContext entityContext, LoggingService loggingService, EntityContextBASE entityContextBASE)
        {
            bool hayElementosEnCola = false;
            int numMaxItems = 10;
            try
            {
                //Comunidades de MyGnoss
                BaseComunidadCN brProyectosCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, null);
                mBaseProyectosDS = (BaseProyectosDS)brProyectosCN.ObtenerElementosColaPendientes(numMaxItems, mSoloPrioridad0);
                brProyectosCN.Dispose();

                hayElementosEnCola = ((mBaseProyectosDS != null) && (mBaseProyectosDS.ColaTagsProyectos.Rows.Count > 0));
            }
            catch (Exception ex)
            {
                GuardarLog(ex, loggingService);
            }

            return hayElementosEnCola;
        }

        protected override void RealizarMantenimientoRabbitMQ(LoggingService loggingService, bool reintentar = true)
        {
            
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItem);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDown);

                RabbitMQClient rMQ = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, "ColaTagsProyectos",loggingService, mConfigService, "", "ColaTagsProyectos");

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

        protected override void RealizarMantenimientoBaseDatosColas()
        {
            //UtilPeticion.AgregarObjetoAPeticionActual("LogActual", mFicheroLog);

            while (true)
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
                    IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication = scope.ServiceProvider.GetRequiredService<IServicesUtilVirtuosoAndReplication>();
                    if (mReiniciarCola)
                    {
                        RealizarMantenimientoRabbitMQ(loggingService);
                        mReiniciarCola = false;
                    }

                    ComprobarCancelacionHilo();
                    EstaHiloActivo = true;

                    try
                    {
                        if (!hayElementosPendientes)
                        {
                            hayElementosPendientes = CargarDatos(entityContext, loggingService, entityContextBASE);
                        }

                        if (hayElementosPendientes)
                        {
                            bool error = false;

                            //Proceso las filas de comunidades de MyGnoss
                            error = ProcesarFilasDeColaDeComunidades(entityContext, loggingService, virtuosoAD, entityContextBASE, redisCacheWrapper, utilidadesVirtuoso, gnossCache, servicesUtilVirtuosoAndReplication);

                            if (error)
                            {
                                //Errores con algun elemento de la cola
                                this.GuardarLog("Ha habido errores en el mantenimiento.", loggingService);
                            }
                        }
                    }
                    catch (OperationCanceledException)
                    {
                        break;
                    }
                    catch (Exception ex)
                    {
                        // Se envía al visor de sucesos una notificación
                        try
                        {
                            string mensaje = "Excepción: " + ex.ToString() + "\n\n\tTraza: " + ex.StackTrace;
                            this.GuardarLog(ex, loggingService);

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

                        //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                        while (!utilidadesVirtuoso.ServidorOperativo(mFicheroConfiguracionBD, mUrlIntragnoss))
                        {
                            //Dormimos 30 segundos
                            Thread.Sleep(30 * 1000);
                        }
                    }
                    finally
                    {
                        if (siguienteBorrado < DateTime.Now)
                        {
                            //Esto solo se ejecuta una vez al día
                            siguienteBorrado = DateTime.Now.AddDays(1);

                            try
                            {
                                BaseComunidadCN brProyectosCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                                brProyectosCN.EliminarElementosColaProcesadosViejos();
                                brProyectosCN.Dispose();
                            }
                            catch (Exception ex)
                            {
                                GuardarLog(ex, loggingService);
                            }
                        }

                        try
                        {
                            //se libera el diccionario de mapeos
                            if (DicUrlMappingProyecto != null)
                            {
                                DicUrlMappingProyecto.Clear();
                                DicUrlMappingProyecto = null;
                            }

                            if (DicPropiedadesOntologia != null)
                            {
                                DicPropiedadesOntologia.Clear();
                                DicPropiedadesOntologia = null;
                            }

                            ControladorConexiones.CerrarConexiones(false);
                            //Compruebo si hay cambios antes de dormir el proceso de nuevo

                            if (hayElementosPendientes)
                            {
                                hayElementosPendientes = CargarDatos(entityContext, loggingService, entityContextBASE);
                            }

                            if (!hayElementosPendientes)
                            {
                                if (mBaseProyectosDS != null)
                                {
                                    mBaseProyectosDS.Dispose();
                                    mBaseProyectosDS = null;
                                }

                                ControladorConexiones.CerrarConexiones(false);

                                //mHoraEnvioErrores
                                //mEmailErrores
                                if (!string.IsNullOrEmpty(mEmailErrores))
                                {
                                    if (utimaEjecucion.Hour < mHoraEnvioErrores && DateTime.Now.Hour == mHoraEnvioErrores)
                                    {
                                        EnviarCorreoErroresUltimas24Horas(mEmailErrores, entityContext, loggingService, entityContextBASE, servicesUtilVirtuosoAndReplication);
                                    }
                                }

                                utimaEjecucion = DateTime.Now;

                                //Duermo el proceso el tiempo establecido
                                Thread.Sleep(INTERVALO_SEGUNDOS * 1000);
                            }
                        }
                        catch (Exception ex)
                        {
                            GuardarLog(ex, loggingService);
                        }
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
                    Debug.WriteLine($"ProcesarItem, {pFila}!");

                    if (!string.IsNullOrEmpty(pFila))
                    {
                        object[] itemArray = JsonConvert.DeserializeObject<object[]>(pFila);
                        BaseProyectosDS.ColaTagsProyectosRow filaCola = (BaseProyectosDS.ColaTagsProyectosRow)new BaseProyectosDS().ColaTagsProyectos.Rows.Add(itemArray);
                        itemArray = null;

                        error = ProcesarFilaDeCola(filaCola, entityContext, loggingService, virtuosoAD, entityContextBASE, redisCacheWrapper, utilidadesVirtuoso, gnossCache, servicesUtilVirtuosoAndReplication);

                        if (!error)
                        {
                            //InsertarColaTagsProyectosAutoCompletar(filaCola, loggingService);
                        }
      

                        filaCola = null;

                        ControladorConexiones.CerrarConexiones(false);
                    }
                }
                catch
                {
                    return false;
                }
                finally
                {
                    GuardarTraza(loggingService);
                }
                return !error;
            }
        }
        /// <summary>
        /// Inserta en la cola de Rabbit los tags de las comunidades
        /// </summary>
        /// <param name="pFilaCola">Parámetro de la fila para la cola</param>
        public void InsertarColaTagsProyectosAutoCompletar(BaseProyectosDS.ColaTagsProyectosRow pFilaCola, LoggingService loggingService)
        {
            string exchange = "";
            string colaRabbit = "ColaTagsProyectos_GeneradorAutocompletar";

            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient rabbitMQ = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, colaRabbit, loggingService, mConfigService, exchange, colaRabbit);
                rabbitMQ.AgregarElementoACola(JsonConvert.SerializeObject(pFilaCola.ItemArray));
                rabbitMQ.Dispose();
            }
        }
        protected bool ProcesarFilasDeColaDeComunidades(EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, EntityContextBASE entityContextBASE, RedisCacheWrapper redisCacheWrapper, UtilidadesVirtuoso utilidadesVirtuoso, GnossCache gnossCache, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            bool error = false;

            //Recorro las filas de comunidades de MyGnoss
            foreach (BaseProyectosDS.ColaTagsProyectosRow filaCola in mBaseProyectosDS.ColaTagsProyectos.Rows)
            {
                //Proceso la fila
                error = error || ProcesarFilaDeCola(filaCola, entityContext, loggingService, virtuosoAD, entityContextBASE, redisCacheWrapper, utilidadesVirtuoso, gnossCache, servicesUtilVirtuosoAndReplication);

                ControladorConexiones.CerrarConexiones(false);
                ComprobarCancelacionHilo();
                EstaHiloActivo = true;
            }

            if (mBaseProyectosDS != null)
            {
                mBaseProyectosDS.Dispose();
                mBaseProyectosDS = null;
            }

            return error;
        }

        protected override ControladorServicioGnoss ClonarControlador()
        {
            ControladorColaComunidades controlador = new ControladorColaComunidades(mReplicacion, mRutaBaseTriplesDescarga, mUrlTriplesDescarga, mEmailErrores, mHoraEnvioErrores, mEscribirFicheroExternoTriples, ScopedFactory, mConfigService);
            return controlador;
        }
    }
}
