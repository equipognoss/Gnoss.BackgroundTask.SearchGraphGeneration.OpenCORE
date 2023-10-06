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
using Es.Riam.AbstractsOpen;

namespace GnossServicioModuloBASE
{
    internal class ControladorColaPerOrg : Controlador
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
        public ControladorColaPerOrg(bool pReplicacion, string pRutaBaseTriplesDescarga, string pUrlTriplesDescarga, string pEmailErrores, int pHoraEnvioErrores, bool pEscribirFicheroExternoTriples, IServiceScopeFactory serviceScope, ConfigService configService, int sleep = 0)
            : base(pReplicacion, pRutaBaseTriplesDescarga, pUrlTriplesDescarga, pEmailErrores, pHoraEnvioErrores, pEscribirFicheroExternoTriples, serviceScope, configService, sleep)
        {
        }

        protected override void RealizarMantenimientoRabbitMQ(LoggingService loggingService,bool reintentar = true)
        {
            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient.ReceivedDelegate funcionProcesarItem = new RabbitMQClient.ReceivedDelegate(ProcesarItem);
                RabbitMQClient.ShutDownDelegate funcionShutDown = new RabbitMQClient.ShutDownDelegate(OnShutDown);

                RabbitMQClient rMQ = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, "ColaTagsCom_Per_Org",loggingService, mConfigService, "", "ColaTagsCom_Per_Org");

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

                        RealizarMantenimientoRabbitMQ(loggingService,false);
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

                            //Proceso las filas de personas y organizaciones
                            error = ProcesarFilasDeColasDePersonasYOrganizaciones(entityContext, loggingService, virtuosoAD, entityContextBASE, redisCacheWrapper, utilidadesVirtuoso, gnossCache, servicesUtilVirtuosoAndReplication);


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
                                if (mBasePerOrgComunidadDS != null)
                                {
                                    mBasePerOrgComunidadDS.Dispose();
                                    mBasePerOrgComunidadDS = null;
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

                    if (!string.IsNullOrEmpty(pFila))
                    {
                        object[] itemArray = JsonConvert.DeserializeObject<object[]>(pFila);
                        BasePerOrgComunidadDS.ColaTagsCom_Per_OrgRow filaCola = (BasePerOrgComunidadDS.ColaTagsCom_Per_OrgRow)new BasePerOrgComunidadDS().ColaTagsCom_Per_Org.Rows.Add(itemArray);
                        itemArray = null;

                        error = ProcesarFilaDeCola(filaCola, entityContext, loggingService, virtuosoAD, entityContextBASE, redisCacheWrapper, utilidadesVirtuoso, gnossCache, servicesUtilVirtuosoAndReplication);
                        if (!error)
                        {

                            InsertarColaTagsComPerOrgAutoCompletar(filaCola, loggingService);
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
                return true;
            }
        }
        /// <summary>
        /// Inserta en la cola de Rabbit los tags de las comunidades
        /// </summary>
        /// <param name="pFilaCola">Parámetro de la fila para la cola</param>
        public void InsertarColaTagsComPerOrgAutoCompletar(BasePerOrgComunidadDS.ColaTagsCom_Per_OrgRow pFilaCola, LoggingService loggingService)
        {
            string exchange = "";
            string colaRabbit = "ColaTagsCom_Per_OrgGeneradorAutocompletar";

            if (mConfigService.ExistRabbitConnection(RabbitMQClient.BD_SERVICIOS_WIN))
            {
                RabbitMQClient rabbitMQ = new RabbitMQClient(RabbitMQClient.BD_SERVICIOS_WIN, colaRabbit, loggingService, mConfigService, exchange, colaRabbit);
                rabbitMQ.AgregarElementoACola(JsonConvert.SerializeObject(pFilaCola.ItemArray));
                rabbitMQ.Dispose();
            }
        }
        /// <summary>
        /// Procesa las filas de personas y organizaciones
        /// </summary>
        /// <returns>Verdad si ha habido algún error</returns>
        protected bool ProcesarFilasDeColasDePersonasYOrganizaciones(EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, EntityContextBASE entityContextBASE, RedisCacheWrapper redisCacheWrapper, UtilidadesVirtuoso utilidadesVirtuoso, GnossCache gnossCache, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            bool error = false;

            //Recorro las filas de personas y organizaciones que participan en una comunidad
            foreach (BasePerOrgComunidadDS.ColaTagsCom_Per_OrgRow filaColaPerOrg in mBasePerOrgComunidadDS.ColaTagsCom_Per_Org.Rows)
            {
                error = error || ProcesarFilaDeCola(filaColaPerOrg, entityContext, loggingService, virtuosoAD, entityContextBASE, redisCacheWrapper, utilidadesVirtuoso, gnossCache, servicesUtilVirtuosoAndReplication);

                ControladorConexiones.CerrarConexiones(false);

                ComprobarCancelacionHilo();

                EstaHiloActivo = true;
            }

            if (mBasePerOrgComunidadDS != null)
            {
                mBasePerOrgComunidadDS.Dispose();
                mBasePerOrgComunidadDS = null;
            }

            return error;
        }

        protected override ControladorServicioGnoss ClonarControlador()
        {
            ControladorColaPerOrg controlador = new ControladorColaPerOrg(mReplicacion, mRutaBaseTriplesDescarga, mUrlTriplesDescarga, mEmailErrores, mHoraEnvioErrores, mEscribirFicheroExternoTriples, ScopedFactory, mConfigService);
            return controlador;
        }

    }
}
