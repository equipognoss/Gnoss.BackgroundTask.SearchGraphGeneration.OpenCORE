using System;
using System.Collections.Generic;
using System.Text;
using System.Threading;
using System.Data;
using Es.Riam.Util.AnalisisSintactico;
using Es.Riam.Gnoss.AD.BASE_BD.Model;
using Es.Riam.Gnoss.AD.BASE_BD;
using Es.Riam.Gnoss.Logica.BASE_BD;
using Es.Riam.Gnoss.Logica.ServiciosGenerales;
using Es.Riam.Gnoss.AD.ServiciosGenerales;
using Es.Riam.Gnoss.Logica.Documentacion;
using Es.Riam.Gnoss.AD.Documentacion;
using Es.Riam.Gnoss.Logica.Facetado;
using Es.Riam.Gnoss.AD.Facetado;
using Es.Riam.Gnoss.CL.Facetado;
using Es.Riam.Gnoss.Logica.Tesauro;
using Es.Riam.Gnoss.Logica.Identidad;
using System.Diagnostics;
using Es.Riam.Gnoss.Logica.Usuarios;
using Es.Riam.Gnoss.CL.ServiciosGenerales;
using Es.Riam.Gnoss.Recursos;
using Es.Riam.Gnoss.Elementos.Tesauro;
using Es.Riam.Interfaces;
using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Logica.ComparticionAutomatica;
using Es.Riam.Gnoss.Logica.CMS;
using Es.Riam.Util;
using Es.Riam.Gnoss.AD.ComparticionAutomatica;
using Es.Riam.Gnoss.Logica.Comentario;
using Es.Riam.Gnoss.Logica.ParametrosProyecto;
using Es.Riam.Gnoss.Util.General;
using Es.Riam.Gnoss.CL.Tesauro;
using Es.Riam.Gnoss.CL.Documentacion;
using Es.Riam.Semantica.OWL;
using Es.Riam.Gnoss.Web.Controles.Documentacion;
using Es.Riam.Gnoss.AD.Parametro;
using Es.Riam.Gnoss.AD.ParametroAplicacion;
using Es.Riam.Gnoss.Web.MVC.Models.Administracion;
using Es.Riam.Gnoss.Web.MVC.Models;
using Es.Riam.Gnoss.AD.EntityModel.Models.ParametroGeneralDS;
using Es.Riam.Gnoss.Elementos.ParametroAplicacion;
using Es.Riam.Gnoss.Web.Controles.ParametroAplicacionGBD;
using Es.Riam.Gnoss.AD.EntityModel;
using System.Linq;
using Es.Riam.Gnoss.AD.EntityModel.Models.ProyectoDS;
using Es.Riam.Gnoss.AD.EncapsuladoDatos;
using Es.Riam.Gnoss.AD.Facetado.Model;
using Es.Riam.Gnoss.Logica.ParametroAplicacion;
using Es.Riam.Gnoss.AD.EntityModelBASE;
using Es.Riam.Gnoss.AD.Virtuoso;
using Microsoft.Extensions.DependencyInjection;
using Es.Riam.Gnoss.Util.Configuracion;
using Es.Riam.Gnoss.CL;
using Es.Riam.Gnoss.UtilServiciosWeb;
using Newtonsoft.Json;
using Es.Riam.Gnoss.AD.Tags;
using Es.Riam.Gnoss.AD.EntityModel.Models.BASE;
using Es.Riam.AbstractsOpen;
using Es.Riam.Gnoss.Util.Seguridad;

namespace GnossServicioModuloBASE
{

    /// <summary>
    /// Controlador que realiza el mantenimiento
    /// </summary>
    internal partial class Controlador : ControladorServicioGnoss
    {
        #region Miembros

        protected BaseRecursosComunidadDS mBaseRecursosComunidadDS = null;
        protected BasePerOrgComunidadDS mBasePerOrgComunidadDS = null;
        protected BaseProyectosDS mBaseProyectosDS = null;
        protected BasePaginaCMSDS mBasePaginaCMSDS = null;


        public StringBuilder mTripletas = new StringBuilder();
        public StringBuilder mTripletasGnoss = new StringBuilder();
        public StringBuilder mTripletasContribuciones = new StringBuilder();
        public StringBuilder mTripletasPerfilPersonal = new StringBuilder();
        public StringBuilder mTripletasPerfilOrganizacion = new StringBuilder();

        protected bool mReplicacion = true;
        protected string mRutaBaseTriplesDescarga = string.Empty;
        protected string mUrlTriplesDescarga = string.Empty;

        protected string mEmailErrores = "";
        protected int mHoraEnvioErrores = 0;

        protected List<Guid> mListadeIDsProyectoSinRegistroObligatorio = new List<Guid>();

        /// <summary>
        /// Obtiene si se trata de un ecosistema sin metaproyecto
        /// </summary>
        protected bool? mEsEcosistemaSinMetaProyecto = null;

        protected string mGrafoMetaBusquedaComunidades = null;
        protected string mGrafoMetaBusquedaPerYOrg = null;
        protected string mGrafoMetaBusquedaRecursos = null;

        protected ParametroGeneral mFilaParametroGeneral;

        protected Dictionary<Guid, string> mDicUrlMappingProyecto;

        /// <summary>
        /// Sirve para escribir en ficheros externos las triples o no.
        /// </summary>
        protected bool mEscribirFicheroExternoTriples;

        protected Dictionary<Guid, Dictionary<string, List<string>>> mDicPropiedadesOntologia = new Dictionary<Guid, Dictionary<string, List<string>>>();

        protected Dictionary<Guid, Ontologia> mListaOntologiasPorID = new Dictionary<Guid, Ontologia>();
        protected Dictionary<Guid, List<ElementoOntologia>> mListaElementosContenedorSuperiorOHerencias = new Dictionary<Guid, List<ElementoOntologia>>();

        protected bool? mSoloPrioridad0;

        protected bool mReiniciarCola = false;

        protected bool hayElementosPendientes = false;

        protected DateTime utimaEjecucion = DateTime.Now;
        protected DateTime siguienteBorrado = DateTime.Now;

        private int mSleepSeconds = 0;



        #endregion

        #region Constructores

        /// <summary>
        /// Constructor de la clase
        /// </summary>
        /// <param name="pFicheroConfiguracionBD">Fichero de configuración de la base de datos</param>
        public Controlador(bool pReplicacion, string pRutaBaseTriplesDescarga, string pUrlTriplesDescarga, string pEmailErrores, int pHoraEnvioErrores, bool pEscribirFicheroExternoTriples, IServiceScopeFactory serviceScopeFactory, ConfigService configService, int sleep = 0)
            : base(serviceScopeFactory, configService)
        {
            mReplicacion = pReplicacion;
            mRutaBaseTriplesDescarga = pRutaBaseTriplesDescarga;
            mUrlTriplesDescarga = pUrlTriplesDescarga;
            mEmailErrores = pEmailErrores;
            mHoraEnvioErrores = pHoraEnvioErrores;
            mEscribirFicheroExternoTriples = pEscribirFicheroExternoTriples;

            mSleepSeconds = sleep;
        }

        #endregion

        #region Metodos generales

        #region publicos

        /// <summary>
        /// Realiza el mantenimiento del módulo BASE
        /// </summary>
        public override void RealizarMantenimiento(EntityContext entityContext, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            Thread.Sleep(mSleepSeconds * 1000);

            GestorParametroAplicacion gestorParametroAplicacion = new GestorParametroAplicacion();
            ParametroAplicacionGBD parametroAplicacionGBD = new ParametroAplicacionGBD(loggingService, entityContext, mConfigService);
            parametroAplicacionGBD.ObtenerConfiguracionGnoss(gestorParametroAplicacion);
            mUrlIntragnoss = gestorParametroAplicacion.ParametroAplicacion.Find(parametroApp => parametroApp.Parametro.Equals("UrlIntragnoss")).Valor;

            FacetaCN facetaCN = new FacetaCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            FacetadoAD facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
            facetaCN.CargarConfiguracionConexionGrafo(facetadoAD.ServidoresGrafo);
            facetaCN.Dispose();

            ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            mListadeIDsProyectoSinRegistroObligatorio = proyCN.ObtenerListaIDsProyectosSinRegistroObligatorio();
            proyCN.Dispose();

            #region Establezco el dominio de la cache

            GestorParametroAplicacion gestorParametroAplicacionCache = new GestorParametroAplicacion();
            ParametroAplicacionGBD parametroAplicacionGBDCache = new ParametroAplicacionGBD(loggingService, entityContext, mConfigService);
            parametroAplicacionGBDCache.ObtenerConfiguracionGnoss(gestorParametroAplicacionCache);

            mDominio = gestorParametroAplicacionCache.ParametroAplicacion.Find(parametroApp => parametroApp.Parametro.Equals("UrlIntragnoss")).Valor;
            mDominio = mDominio.Replace("http://", "").Replace("www.", "");

            if (mDominio[mDominio.Length - 1] == '/')
            {
                mDominio = mDominio.Substring(0, mDominio.Length - 1);
            }
            #endregion

            RealizarMantenimientoRabbitMQ(loggingService);

            //RealizarMantenimientoBaseDatosColas();

        }

        protected virtual void RealizarMantenimientoRabbitMQ(LoggingService loggingService, bool reintentar = true)
        {
            throw new Exception("NOT IMPLEMENTED");
        }

        /// <summary>
        /// Carga los mantenimientos pendientes
        /// </summary>
        /// <returns>Verdad si hay algún elemento que procesar</returns>
        protected virtual bool CargarDatos(EntityContext entityContext, LoggingService loggingService, EntityContextBASE entityContextBASE)
        {
            throw new Exception("NOT IMPLEMENTED");
        }

        protected virtual void RealizarMantenimientoBaseDatosColas()
        {
            throw new Exception("NOT IMPLEMENTED");
        }

        protected void OnShutDown()
        {
            mReiniciarCola = true;
        }


        #endregion

        #region privados

        #region Manipulación de relaciones de tags

        #region Cola Tags MyGnoss

        protected string ObtenerTripletasCategoriasProyecto(Guid pProyectoID, ref string pCampoSearch, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            DataWrapperProyecto dataWrapperProyecto = proyCN.ObtenerProyectoPorID(pProyectoID);
            proyCN.Dispose();

            string tripletas = CrearTripletasCategoriasElementoDWProyecto(pProyectoID, dataWrapperProyecto.ListaProyectoAgCatTesauro, ProyectoAD.MetaProyecto, ref pCampoSearch, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);

            return tripletas;
        }

        protected string ObtenerTripletasCategoriasRecurso(Guid pDocumentoID, Guid pProyectoID, ref string pCampoSearch, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            int pNumeroTripletas = 0;

            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            List<Guid> listaDocsID = new List<Guid>();
            listaDocsID.Add(pDocumentoID);
            List<Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.DocumentoWebAgCatTesauroConVinculoTesauroID> listaDocumentoWebAgCatTesauroConVinculoTesauroID = docCN.ObtenerCategoriasTesauroYTesauroDeDocumentos(listaDocsID);
            docCN.Dispose();

            string tripletas = CrearTripletasCategoriasElemento(pDocumentoID, listaDocumentoWebAgCatTesauroConVinculoTesauroID, pProyectoID, ref pNumeroTripletas, ref pCampoSearch, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);
            return tripletas;
        }

        protected void AgregarTripletasDescompuestas(Guid id, Guid proyid, string pPropiedad, string pTextoSinSeparar, bool pActualizarTriplesGnoss, bool pActualizarTriplesContribuciones, List<string> pListaTagsDocumento, bool pTriplesRecursoYaAgregado = false)
        {
            if (!pPropiedad.Equals("<http://gnoss/hasTagTituloDesc>") || !pListaTagsDocumento.Contains(pTextoSinSeparar))
            {
                string triples = UtilidadesVirtuoso.AgregarTripletasDescompuestasTitulo(id.ToString(), pPropiedad, pTextoSinSeparar);

                /*if ((mTripletasGnoss != null) && (proyid.Equals(ProyectoAD.MetaProyecto.ToString())) && pActualizarTriplesGnoss)
                {
                    mTripletasGnoss.Append(triples);
                }*/

                if (!pTriplesRecursoYaAgregado)
                {
                    mTripletas.Append(triples);
                }

                if (pActualizarTriplesContribuciones)
                {
                    mTripletasContribuciones.Append(triples);
                }
            }
        }

        protected string CrearTripletasCategoriasElementoDWProyecto(Guid pElementoID, List<ProyectoAgCatTesauro> pFilasRelacionCategoria, Guid pProyectoTesauroID, ref string pCampoSearch, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            int num = 0;
            return CrearTripletasCategoriasElementooDWProyecto(pElementoID, pFilasRelacionCategoria, pProyectoTesauroID, ref num, ref pCampoSearch, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);
        }

        protected string CrearTripletasCategoriasElementooDWProyecto(Guid pElementoID, List<ProyectoAgCatTesauro> pFilasRelacionCategoria, Guid pProyectoTesauroID, ref int pNumeroTripletas, ref string pCampoSearch, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            //obtener tesauro
            //TesauroCN tesCN = new TesauroCN(mFicheroConfiguracionBD, false);
            TesauroCL tesCL = new TesauroCL(mFicheroConfiguracionBD, entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
            tesCL.Dominio = mDominio;
            GestionTesauro gestorTesauro = null;
            if (pProyectoTesauroID.Equals(ProyectoAD.MetaProyecto))
            {
                gestorTesauro = new GestionTesauro(tesCL.ObtenerTesauroDeProyectoMyGnoss(), loggingService, entityContext);
            }
            else
            {
                gestorTesauro = new GestionTesauro(tesCL.ObtenerTesauroDeProyecto(pProyectoTesauroID), loggingService, entityContext);
            }

            tesCL.Dispose();

            List<Guid> categoriasAgregadas = new List<Guid>();

            foreach (ProyectoAgCatTesauro filaAgCat in pFilasRelacionCategoria)
            {
                Guid idCat = filaAgCat.CategoriaTesauroID;
                if (!categoriasAgregadas.Contains(idCat))
                {
                    categoriasAgregadas.Add(idCat);

                    //while padre
                    List<Es.Riam.Gnoss.AD.EntityModel.Models.Tesauro.CatTesauroAgCatTesauro> filasCatAgCat = gestorTesauro.TesauroDW.ListaCatTesauroAgCatTesauro.Where(catTesAgCatTes => catTesAgCatTes.CategoriaInferiorID.Equals(idCat)).ToList();
                    while (filasCatAgCat.Count > 0)
                    {
                        Guid catPadreID = filasCatAgCat[0].CategoriaSuperiorID;
                        if (!categoriasAgregadas.Contains(catPadreID))
                        {
                            categoriasAgregadas.Add(catPadreID);
                        }
                        filasCatAgCat = gestorTesauro.TesauroDW.ListaCatTesauroAgCatTesauro.Where(catTesAgCatTes => catTesAgCatTes.CategoriaInferiorID.Equals(catPadreID)).ToList();
                    }
                }
            }



            string texto = "";
            string sujeto = "<http://gnoss/" + pElementoID.ToString().ToUpper() + "> ";
            string predicado = "<http://www.w3.org/2004/02/skos/core#ConceptID> ";
            foreach (Guid categoriaID in categoriasAgregadas)
            {
                if (gestorTesauro.ListaCategoriasTesauro.ContainsKey(categoriaID))
                {
                    string objeto = "<http://gnoss/" + categoriaID.ToString().ToUpper() + "> .";

                    texto += sujeto + predicado + "<http://gnoss/" + categoriaID.ToString().ToUpper() + "> . \n ";
                    pNumeroTripletas++;

                    //string nombrecategoria = tesCN.ObtenerNombreCategoriaPorID(categoriaID);
                    string nombrecategoria = gestorTesauro.ListaCategoriasTesauro[categoriaID].FilaCategoria.Nombre;


                    texto += "<http://gnoss/" + categoriaID.ToString().ToUpper() + ">" + "<http://gnoss/CategoryName>" + "\"" + nombrecategoria.ToLower() + "\" . \n ";
                    pNumeroTripletas++;

                    Dictionary<string, string> idiomaCategoria = UtilCadenas.ObtenerTextoPorIdiomas(nombrecategoria.ToLower());

                    if (idiomaCategoria.Count > 0)
                    {
                        List<string> idiomasSimilares = new List<string>();

                        foreach (string idioma in idiomaCategoria.Keys)
                        {
                            if (!idiomasSimilares.Contains(idiomaCategoria[idioma]))
                            {
                                pCampoSearch += " " + idiomaCategoria[idioma];
                                idiomasSimilares.Add(idiomaCategoria[idioma]);
                            }
                        }
                    }
                    else
                    {
                        pCampoSearch += " " + nombrecategoria.ToLower();
                    }
                }
            }

            gestorTesauro.Dispose();
            return texto;
        }

        protected string CrearTripletasCategoriasElemento(Guid pElementoID, List<Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.DocumentoWebAgCatTesauroConVinculoTesauroID> pFilasRelacionCategoria, Guid pProyectoTesauroID, ref string pCampoSearch, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            int num = 0;
            return CrearTripletasCategoriasElemento(pElementoID, pFilasRelacionCategoria, pProyectoTesauroID, ref num, ref pCampoSearch, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);
        }

        protected string CrearTripletasCategoriasElemento(Guid pElementoID, List<Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.DocumentoWebAgCatTesauroConVinculoTesauroID> pFilasRelacionCategoria, Guid pProyectoTesauroID, ref int pNumeroTripletas, ref string pCampoSearch, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            //Obtener tesauro
            TesauroCL tesCL = new TesauroCL(mFicheroConfiguracionBD, entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
            tesCL.Dominio = mDominio;
            GestionTesauro gestorTesauro = null;
            if (pProyectoTesauroID.Equals(ProyectoAD.MetaProyecto))
            {
                gestorTesauro = new GestionTesauro(tesCL.ObtenerTesauroDeProyectoMyGnoss(), loggingService, entityContext);
            }
            else
            {
                gestorTesauro = new GestionTesauro(tesCL.ObtenerTesauroDeProyecto(pProyectoTesauroID), loggingService, entityContext);
            }

            tesCL.Dispose();

            List<Guid> categoriasAgregadas = new List<Guid>();

            foreach (Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.DocumentoWebAgCatTesauroConVinculoTesauroID filaAgCat in pFilasRelacionCategoria)
            {
                Guid idCat = filaAgCat.CategoriaTesauroID;
                if (!categoriasAgregadas.Contains(idCat))
                {
                    categoriasAgregadas.Add(idCat);

                    //while padre 
                    List<Es.Riam.Gnoss.AD.EntityModel.Models.Tesauro.CatTesauroAgCatTesauro> filasCatAgCat = gestorTesauro.TesauroDW.ListaCatTesauroAgCatTesauro.Where(catAgCat => catAgCat.CategoriaInferiorID.Equals(idCat)).ToList();
                    while (filasCatAgCat.Count > 0)
                    {
                        Guid catPadreID = filasCatAgCat[0].CategoriaSuperiorID;
                        if (!categoriasAgregadas.Contains(catPadreID))
                        {
                            categoriasAgregadas.Add(catPadreID);
                        }
                        filasCatAgCat = gestorTesauro.TesauroDW.ListaCatTesauroAgCatTesauro.Where(catAgCat => catAgCat.CategoriaInferiorID.Equals(catPadreID)).ToList();
                    }
                }
            }

            string texto = "";
            string sujeto = $"<http://gnoss/{pElementoID}> ";
            string predicado = "<http://www.w3.org/2004/02/skos/core#ConceptID> ";
            foreach (Guid categoriaID in categoriasAgregadas)
            {
                if (gestorTesauro.ListaCategoriasTesauro.ContainsKey(categoriaID))
                {
                    string objeto = $"<http://gnoss/{categoriaID.ToString().ToUpper()}> .";

                    texto += $"{sujeto}{predicado}<http://gnoss/{categoriaID.ToString().ToUpper()}> . \n ";
                    pNumeroTripletas++;

                    //string nombrecategoria = tesCN.ObtenerNombreCategoriaPorID(categoriaID);
                    string nombrecategoria = gestorTesauro.ListaCategoriasTesauro[categoriaID].FilaCategoria.Nombre;


                    texto += $"<http://gnoss/{categoriaID.ToString().ToUpper()}> <http://gnoss/CategoryName> \"{nombrecategoria.ToLower()}\" . \n ";
                    pNumeroTripletas++;

                    Dictionary<string, string> idiomaCategoria = UtilCadenas.ObtenerTextoPorIdiomas(nombrecategoria.ToLower());

                    if (idiomaCategoria.Count > 0)
                    {
                        List<string> idiomasSimilares = new List<string>();

                        foreach (string idioma in idiomaCategoria.Keys)
                        {
                            if (!idiomasSimilares.Contains(idiomaCategoria[idioma]))
                            {
                                pCampoSearch += " " + idiomaCategoria[idioma];
                                idiomasSimilares.Add(idiomaCategoria[idioma]);
                            }
                        }
                    }
                    else
                    {
                        pCampoSearch += " " + nombrecategoria.ToLower();
                    }
                }
            }

            gestorTesauro.Dispose();
            return texto;
        }

        #endregion


        /// <summary>
        /// Procesa una fila de la cola, calcula sus tags y actualiza la Base de Datos del modelo BASE
        /// </summary>
        /// <param name="pFila">Fila de cola a procesar</param>
        /// <returns>Verdad si ha habido algun error durante la operación</returns>
        protected bool ProcesarFilaDeCola(DataRow pFila, EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, EntityContextBASE entityContextBASE, RedisCacheWrapper redisCacheWrapper, UtilidadesVirtuoso utilidadesVirtuoso, GnossCache gnossCache, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            return ProcesarFilaDeCola(pFila, null, null, null, null, null, null, null, null, null, null, null, null, entityContext, loggingService, virtuosoAD, entityContextBASE, redisCacheWrapper, utilidadesVirtuoso, gnossCache, servicesUtilVirtuosoAndReplication);
        }


        /// <summary>
        /// Procesa una fila de la cola, calcula sus tags y actualiza la Base de Datos del modelo BASE
        /// </summary>
        /// <param name="pFila">Fila de cola a procesar</param>
        /// <returns>Verdad si ha habido algun error durante la operación</returns>
        protected bool ProcesarFilaDeCola(DataRow pFila, Proyecto pFilaProyecto, bool? pTieneProyectoComponenteConCaducidadRecurso, List<string> listaTagsDirectos, List<string> listaTagsIndirectos, Dictionary<short, List<string>> listaTagsFiltros, List<string> listaTodosTags, DataWrapperFacetas tConfiguracionDS, Dictionary<Guid, bool> pListaDocsBorradores, List<string> listaTags, string pTitulo, string pDescripcion, Dictionary<Guid, Dictionary<Guid, FacetaDS>> pDiccionarioProyectoDocInformacionComunRecurso, EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, EntityContextBASE entityContextBASE, RedisCacheWrapper redisCacheWrapper, UtilidadesVirtuoso utilidadesVirtuoso, GnossCache gnossCache, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            bool error = false;

            try
            {
                short estado = (short)pFila["Estado"];
                if (estado < 2)
                {
                    //Obtengo los tags en una lista
                    if (listaTagsDirectos == null)
                    {
                        listaTagsDirectos = new List<string>();
                    }
                    if (listaTagsIndirectos == null)
                    {
                        listaTagsIndirectos = new List<string>();
                    }
                    if (listaTagsFiltros == null)
                    {
                        listaTagsFiltros = new Dictionary<short, List<string>>();
                    }
                    if (listaTodosTags == null)
                    {
                        listaTodosTags = SepararTags((string)pFila["Tags"], listaTagsDirectos, listaTagsIndirectos, listaTagsFiltros, pFila.Table.DataSet);
                    }
                    bool agregarTagsAModeloBase = true;
                    mTripletas.Clear();
                    mTripletasGnoss.Clear();
                    mTripletasContribuciones.Clear();
                    mTripletasPerfilPersonal.Clear();
                    mTripletasPerfilOrganizacion.Clear();


                    //string triplesSearch;
                    //si lo hago con triples, cada vez que genere algo
                    //si lo hago con una consulta, traigo lo básico más lo del formulario semántico
                    //según el tipo de item, tengo que traer unas propiedades u otras
                    if (pFila["Tipo"].Equals((short)TiposElementosEnCola.Agregado)
                        || pFila["Tipo"].Equals((short)TiposElementosEnCola.InsertadoEnGrafoBusquedaDesdeWeb))
                    {
                        ProcesarFilaDeColaDeTipoAgregado(ref pFila, listaTodosTags, listaTagsDirectos, listaTagsIndirectos, listaTagsFiltros, ref agregarTagsAModeloBase, pFilaProyecto, pTieneProyectoComponenteConCaducidadRecurso, tConfiguracionDS, pListaDocsBorradores, listaTags, pTitulo, pDescripcion, pDiccionarioProyectoDocInformacionComunRecurso, entityContext, loggingService, virtuosoAD, utilidadesVirtuoso, entityContextBASE, redisCacheWrapper, gnossCache, servicesUtilVirtuosoAndReplication);
                    }
                    else if (pFila["Tipo"].Equals((short)TiposElementosEnCola.Eliminado))
                    {
                        ProcesarFilaDeColaDeTipoEliminado(ref pFila, listaTodosTags, listaTagsDirectos, listaTagsIndirectos, listaTagsFiltros, entityContext, loggingService, virtuosoAD, entityContextBASE, redisCacheWrapper, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                    }
                    else if (pFila["Tipo"].Equals((short)TiposElementosEnCola.NivelesCertificacionModificados))
                    {
                        ProcesarFilaDeColaDeTipoNivelesCertificacionModificados(ref pFila, entityContext, loggingService, redisCacheWrapper, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    }
                    else if ((pFila["Tipo"].Equals((short)TiposElementosEnCola.CategoriaEliminadaSinRecategorizar))
                        || (pFila["Tipo"].Equals((short)TiposElementosEnCola.CategoriaEliminadaRecategorizarTodo))
                        || (pFila["Tipo"].Equals((short)TiposElementosEnCola.CategoriaEliminadaRecategorizarHuerfanos)))
                    {
                        ProcesarFilaDeColaDeCategoriasRecategorizadas(ref pFila, listaTagsFiltros, entityContext, loggingService, redisCacheWrapper, virtuosoAD, entityContextBASE, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                    }

                    pFila["Estado"] = (short)EstadosColaTags.Procesado;

                    if (pFila is Es.Riam.Gnoss.AD.BASE_BD.Model.BaseRecursosComunidadDS.ColaTagsComunidadesRow)
                    {
                        BaseComunidadCN baseComunidadCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, mServicesUtilVirtuosoAndReplication);

                        if (baseComunidadCN.ExisteColaRabbit("ColaTagsComunidadesLinkedData"))
                        {
                            ProyectoCN proyectoCN = new ProyectoCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                            Guid proyectoID = ProyectoAD.MetaProyecto;

                            if ((int)pFila["TablaBaseProyectoID"] != 0)
                            {
                                DataWrapperProyecto dataWrapperProyecto = proyectoCN.ObtenerProyectoPorTablaBaseProyectoID((int)pFila["TablaBaseProyectoID"]);
                                proyectoID = dataWrapperProyecto.ListaProyecto.FirstOrDefault().ProyectoID;

                            }

                            ControladorDocumentacion controladorDocumentacion = new ControladorDocumentacion(loggingService, entityContext, mConfigService, redisCacheWrapper, gnossCache, entityContextBASE, virtuosoAD, null, servicesUtilVirtuosoAndReplication);
                            controladorDocumentacion.InsertLinkedDataRabbit(proyectoID, (string)pFila["Tags"]);
                        }
                    }
                }
            }
            catch (Exception exFila)
            {
                //Ha habido algún error durante la operación, notifico el error
                error = true;

                string mensaje = "Excepción: " + exFila.ToString() + "\n\n\tTraza: " + exFila.StackTrace + "\n\nFila: " + pFila["Tags"];
                loggingService.GuardarLogError("ERROR:  " + mensaje);

                pFila["Estado"] = ((short)pFila["Estado"]) + 1; //Aumento en 1 el error, cuando llegue a 2 no se volverá a intentar

                if (pFila is Es.Riam.Gnoss.AD.BASE_BD.Model.BaseRecursosComunidadDS.ColaTagsComunidadesRow)
                {
                    ComprobarFilasRepetidasDataSet((BaseRecursosComunidadDS)pFila.Table.DataSet, (string)pFila["Tags"], (short)pFila["Tipo"], (int)pFila["TablaBaseProyectoID"], ((short)pFila["Estado"]) + 1);
                }

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
                pFila["FechaProcesado"] = DateTime.Now;
            }

            return error;
        }

        protected void ComprobarFilasRepetidasDataSet(BaseRecursosComunidadDS pBaseRecursosComunidadDS, string pTags, short pTipo, int pTablaBaseProyectoID, int pEstado)
        {
            if (pBaseRecursosComunidadDS != null && pBaseRecursosComunidadDS.ColaTagsComunidades != null)
            {
                DataRow[] drs = pBaseRecursosComunidadDS.ColaTagsComunidades.Select("Tags = '" + pTags + "' AND Estado <> " + pEstado + " AND Tipo = " + pTipo + " AND TablaBaseProyectoID = " + pTablaBaseProyectoID);
                foreach (DataRow dr in drs)
                {
                    dr["Estado"] = pEstado;
                }
            }
        }

        #region ProcesarFilaDeColaDeTipoAgregado

        protected void ProcesarFilaDeColaDeTipoAgregado(ref DataRow pFila, List<string> listaTodosTags, List<string> listaTagsDirectos, List<string> listaTagsIndirectos, Dictionary<short, List<string>> listaTagsFiltros, ref bool agregarTagsAModeloBase, Proyecto pFilaProyecto, bool? pTieneProyectoComponenteConCaducidadRecurso, DataWrapperFacetas facetaDW, Dictionary<Guid, bool> pListaDocsBorradores, List<string> listaTags, string pTitulo, string pDescripcion, Dictionary<Guid, Dictionary<Guid, FacetaDS>> pDiccionarioProyectoDocInformacionComunRecurso, EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, UtilidadesVirtuoso utilidadesVirtuoso, EntityContextBASE entityContextBASE, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetaDS facetaDS = new FacetaDS();
            List<string> listaTablasMantenerConfiguracion = new List<string>();
            listaTablasMantenerConfiguracion.Add("TripletasExtraContribucionesRecurso");

            Dictionary<string, string> listaIdsEliminar = new Dictionary<string, string>();

            ActualizacionFacetadoCN actualizacionFacetadoCN = new ActualizacionFacetadoCN(mFicheroConfiguracionBD, mUrlIntragnoss, entityContext, loggingService, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
            if (facetaDW == null)
            {
                facetaDW = new DataWrapperFacetas();
            }

            ProyectoCN proyectoCN = new ProyectoCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            Guid proyID = ProyectoAD.MetaProyecto;
            Proyecto filaProyecto = null;

            bool TieneComponenteConCaducidadTipoRecurso = false;

            if ((int)pFila["TablaBaseProyectoID"] != 0)
            {
                if (pFilaProyecto != null)
                {
                    filaProyecto = pFilaProyecto;
                }
                else
                {
                    DataWrapperProyecto dataWrapperProyecto = proyectoCN.ObtenerProyectoPorTablaBaseProyectoID((int)pFila["TablaBaseProyectoID"]);

                    if (dataWrapperProyecto != null && dataWrapperProyecto.ListaProyecto.Count > 0)
                    {
                        filaProyecto = dataWrapperProyecto.ListaProyecto.FirstOrDefault();
                    }
                    else
                    {
                        throw new Exception($"No se encontró el proyecto con TablaBaseProyectoID {pFila["TablaBaseProyectoID"]}. Puede que no exista o esté cerrado. ");
                    }
                }

                proyID = filaProyecto.ProyectoID;

                if (pTieneProyectoComponenteConCaducidadRecurso.HasValue)
                {
                    TieneComponenteConCaducidadTipoRecurso = pTieneProyectoComponenteConCaducidadRecurso.Value;
                }
                else
                {
                    CMSCN cmsCN = new CMSCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                    TieneComponenteConCaducidadTipoRecurso = cmsCN.ObtenerSiTieneComponenteConCaducidadTipoRecurso(filaProyecto.ProyectoID);
                    cmsCN.Dispose();
                }
            }

            Guid organizacionID = Guid.Empty;

            if (filaProyecto != null)
            {
                organizacionID = filaProyecto.OrganizacionID;
            }
            else
            {
                ProyectoCN proyCN = new ProyectoCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                organizacionID = proyCN.ObtenerOrganizacionIDProyecto(proyID);
            }

            #region Actualizar facetado

            List<string> tags = new List<string>();
            string valorSearch = "";

            Guid? id = null;
            bool esDocSemantico = false;
            if (pFila.Table.DataSet is BaseRecursosComunidadDS)
            {
                id = ProcesarFilaDeColaDeTipoAgregadoRecursos(pFila, listaTagsFiltros, ref listaIdsEliminar, ref facetaDW, actualizacionFacetadoCN, proyID, ref valorSearch, ref tags, ref agregarTagsAModeloBase, TieneComponenteConCaducidadTipoRecurso, filaProyecto, organizacionID, listaTablasMantenerConfiguracion, pListaDocsBorradores, listaTags, pTitulo, pDescripcion, pDiccionarioProyectoDocInformacionComunRecurso, ref esDocSemantico, entityContext, loggingService, redisCacheWrapper, entityContextBASE, virtuosoAD, gnossCache, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
            }
            else if (pFila.Table.DataSet is BasePerOrgComunidadDS)
            {
                id = ProcesarFilaDeColaDeTipoAgregadoPersonasYOrganizaciones(pFila, listaTagsFiltros, ref listaIdsEliminar, ref facetaDW, actualizacionFacetadoCN, proyID, ref valorSearch, ref tags, ref agregarTagsAModeloBase, filaProyecto, entityContext, loggingService, virtuosoAD, entityContextBASE, redisCacheWrapper, utilidadesVirtuoso, gnossCache, servicesUtilVirtuosoAndReplication);
            }
            else if (pFila.Table.DataSet is BaseProyectosDS)
            {
                id = ProcesarFilaDeColaDeTipoAgregadoProyectos(pFila, listaTagsFiltros, ref facetaDW, actualizacionFacetadoCN, proyID, ref valorSearch, ref tags, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);
            }
            else if (pFila.Table.DataSet is BasePaginaCMSDS)
            {
                // Método para generar los triples de la pagina del CMS
                id = ProcesarFilaDeColaDeTipoAgregadoPaginaCMS(listaTagsFiltros, ref facetaDW, actualizacionFacetadoCN, proyID, ref valorSearch, entityContext, loggingService, redisCacheWrapper, virtuosoAD, servicesUtilVirtuosoAndReplication);
            }

            bool tripletasYaAgregadas = pFila["Tipo"].Equals((short)TiposElementosEnCola.InsertadoEnGrafoBusquedaDesdeWeb);

            //Añado el campo search
            foreach (string tag in tags)
            {
                valorSearch += " " + tag;
            }

            string tripleSearch = null;
            DataWrapperProyecto dataWrapperProyectoAccionesExternas = proyectoCN.ObtenerAccionesExternasProyectoPorProyectoID(proyID);

            if (mTripletas.Length > 0 || tripletasYaAgregadas)
            {
                string grafo = proyID.ToString();

                if (id.HasValue && !listaIdsEliminar.ContainsKey(id.Value.ToString()))
                {
                    listaIdsEliminar.Add(id.Value.ToString(), "");
                }

                if (EsEcosistemaSinMetaProyecto && proyID.Equals(ProyectoAD.MetaProyecto))
                {
                    if (pFila is BaseProyectosDS.ColaTagsProyectosRow)
                    {
                        if (!GrafoMetaBusquedaComunidades.Equals(string.Empty))
                        {
                            grafo = GrafoMetaBusquedaComunidades;
                        }
                    }
                    else if (pFila is BasePerOrgComunidadDS.ColaTagsCom_Per_OrgRow)
                    {
                        if (!GrafoMetaBusquedaPerYOrg.Equals(string.Empty))
                        {
                            grafo = GrafoMetaBusquedaPerYOrg;
                        }
                    }
                    else if (pFila is Es.Riam.Gnoss.AD.BASE_BD.Model.BaseRecursosComunidadDS.ColaTagsComunidadesRow)
                    {
                        if (!GrafoMetaBusquedaRecursos.Equals(string.Empty))
                        {
                            grafo = GrafoMetaBusquedaRecursos;
                        }
                    }
                }

                if (!tripletasYaAgregadas)
                {
                    InsertaTripletasConModify_ControlCheckPoint(ObtenerPrioridadFila(pFila), grafo, mTripletas.ToString(), listaIdsEliminar, "", "", esDocSemantico, loggingService, entityContext, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                }
                //else
                //{
                //    //el search solo se regenera para los semánticos. El resto lo genera la web directamente
                //    if (esDocSemantico)
                //    {
                //        valorSearch = utilidadesVirtuoso.LeerSearchDeVirtuoso(id.Value, proyID, mUrlIntragnoss) + valorSearch;
                //    }
                //}

                mTripletas.Clear();

                if (id.HasValue && (!tripletasYaAgregadas || esDocSemantico))
                {
                    AccionesExternasProyecto accionExternaSearch = dataWrapperProyectoAccionesExternas.ListaAccionesExternasProyecto.FirstOrDefault(item => item.TipoAccion.Equals((short)TipoAccionExterna.GenerarSearch) && item.ProyectoID.Equals(proyID));
                    if (accionExternaSearch != null && pFila.Table.DataSet is BaseRecursosComunidadDS)
                    {
                        loggingService.AgregarEntrada("Search: se llama a un api externo para generar el search");
                        string search = CallWebMethods.CallGetApi(accionExternaSearch.URL, $"?id={id.Value}");
                        loggingService.AgregarEntrada("Search: Fin de la llamada al api externo para general al search");
                        valorSearch = $"{valorSearch} {search}";
                    }
                    if (tripleSearch == null)
                    {
                        tripleSearch = ObtenerTipleSearch(valorSearch, id.Value, proyID, pFila.Table.DataSet is BaseRecursosComunidadDS, entityContext, loggingService, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                    }
                    
                    utilidadesVirtuoso.InsertarTriplesEdicionTagsCategoriasSearchRecurso(id.Value, proyID, tripleSearch, mUrlIntragnoss, (PrioridadBase)(short)pFila["Prioridad"], false, false);
                }
            }

            ParametroAplicacion filaParametro = GestorParametroAplicacionDS.ParametroAplicacion.Find(parametroApp => parametroApp.Parametro.Equals(TiposParametrosAplicacion.GenerarGrafoContribuciones));
            bool generarGrafoContribuciones = (filaParametro == null || filaParametro.Valor.Equals("1"));

            if (mTripletasPerfilOrganizacion.Length > 0)
            {
                Dictionary<Guid, Guid> DicIDUS = new Dictionary<Guid, Guid>();
                DocumentacionCN docCN = new DocumentacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                if (id.HasValue && docCN.EsDocumentoBorrador(id.Value))
                {
                    if (!listaIdsEliminar.ContainsKey(id.ToString()))
                    {
                        listaIdsEliminar.Add(id.ToString(), "");
                    }
                    string sujeto = "<http://gnoss/" + id.Value.ToString().ToUpper() + ">";
                    string predicadoprivacidad = "<http://gnoss/hasprivacidadMyGnoss> ";
                    string privacidad = "\"publico\" .";
                    mTripletasPerfilOrganizacion.Append(FacetadoAD.GenerarTripleta(sujeto, predicadoprivacidad, privacidad));
                }

                // Devuelve a todos aquellos que los tengan en su BR
                if (id.HasValue)
                {
                    DicIDUS = docCN.ObtenerIdentidadyOrganizacionIDdeRecurso(id.Value);
                }
                foreach (KeyValuePair<Guid, Guid> pp in DicIDUS)
                {
                    //Busco las tripletas extra
                    string tripletasextra = "";

                    #region privacidad my gnoss

                    //Obtenemos la privacidad de la persona
                    OrganizacionCN organizacionCN = new OrganizacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                    DataWrapperOrganizacion organizacion2DW = organizacionCN.ObtenerOrganizacionPorID(pp.Value);

                    string publico = "\"privado\" .";

                    if (organizacion2DW.ListaConfiguracionGnossOrg.FirstOrDefault().VerRecursosExterno)
                    {
                        publico = "\"publico\" .";
                    }
                    else if (organizacion2DW.ListaConfiguracionGnossOrg.FirstOrDefault().VerRecursos)
                    {
                        publico = "\"publicoreg\" .";
                    }

                    //Obtenemos las relaciones con el tesauro del documento
                    DataWrapperDocumentacion docDW = null;
                    Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.Documento filaDocumento = null;

                    if (id.HasValue)
                    {
                        docDW = docCN.ObtenerDocumentoDocumentoWebVinBRPorIDDeOrganizacion(id.Value, pp.Value);
                        filaDocumento = docDW.ListaDocumento.Where(doc => doc.DocumentoID.Equals(id.Value)).ToList().FirstOrDefault();
                    }

                    bool privado = true;

                    if (docDW != null && docDW.ListaDocumentoWebAgCatTesauro != null && docDW.ListaDocumentoWebAgCatTesauro.Count > 0)
                    {
                        Guid tesauroID = docDW.ListaDocumentoWebAgCatTesauro.First().TesauroID;
                        List<Guid> categoriasAgregadas = new List<Guid>();

                        //Obtenemos el tesauro
                        TesauroCN tesCN = new TesauroCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                        DataWrapperTesauro tesDW = tesCN.ObtenerTesauroCompletoPorID(tesauroID);
                        tesDW.Merge(tesCN.ObtenerTesauroOrganizacion(pp.Value));
                        tesCN.Dispose();

                        foreach (Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.DocumentoWebAgCatTesauro filaDocAgCat in docDW.ListaDocumentoWebAgCatTesauro)
                        {
                            if (!categoriasAgregadas.Contains(filaDocAgCat.CategoriaTesauroID))
                            {
                                categoriasAgregadas.Add(filaDocAgCat.CategoriaTesauroID);

                                //while padre       
                                List<Es.Riam.Gnoss.AD.EntityModel.Models.Tesauro.CatTesauroAgCatTesauro> filasCatAgCat = tesDW.ListaCatTesauroAgCatTesauro.Where(catTes => catTes.CategoriaInferiorID.Equals(filaDocAgCat.CategoriaTesauroID)).ToList();
                                while (filasCatAgCat.Count > 0)
                                {
                                    Guid catPadreID = filasCatAgCat[0].CategoriaSuperiorID;
                                    if (!categoriasAgregadas.Contains(catPadreID))
                                    {
                                        categoriasAgregadas.Add(catPadreID);
                                    }
                                    filasCatAgCat = tesDW.ListaCatTesauroAgCatTesauro.Where(catTes => catTes.CategoriaInferiorID.Equals(catPadreID)).ToList();
                                }
                            }
                        }

                        //Inserto las filas de las relaciones con categorias:
                        string sujeto = "<http://gnoss/" + id.Value.ToString().ToUpper() + ">";
                        string predicado = "<http://www.w3.org/2004/02/skos/core#ConceptID>";
                        foreach (Guid categoriaID in categoriasAgregadas)
                        {
                            string objeto = "<http://gnoss/" + categoriaID.ToString().ToUpper() + "> .";

                            mTripletasPerfilOrganizacion.Append(FacetadoAD.GenerarTripleta(sujeto, predicado, objeto));

                            mTripletasPerfilOrganizacion.Append(FacetadoAD.GenerarTripleta("<http://gnoss/" + categoriaID.ToString().ToUpper() + "> ", "<http://rdfs.org/sioc/ns#has_space>", "<http://gnoss/11111111-1111-1111-1111-111111111111> ."));
                        }

                        //Inserto la privacidad del recurso
                        if (filaDocumento.UltimaVersion && !filaDocumento.Borrador && !filaDocumento.Eliminado)
                        {
                            foreach (Guid categoriaID in categoriasAgregadas)
                            {
                                if (tesDW.ListaTesauroOrganizacion.Count > 0 && categoriaID == tesDW.ListaTesauroOrganizacion.FirstOrDefault().CategoriaTesauroPublicoID)
                                {
                                    privado = false;
                                }
                            }
                        }

                        string privacidad = "\"privado\" .";
                        if (!privado)
                        {
                            privacidad = publico;
                        }

                        string predicadoprivacidad = "<http://gnoss/hasprivacidadMyGnoss> ";
                        mTripletasPerfilOrganizacion.Append(FacetadoAD.GenerarTripleta(sujeto, predicadoprivacidad, privacidad));
                    }

                    #region autores(aparte del check)

                    if (filaDocumento.Autor != null)
                    {
                        string[] autores = filaDocumento.Autor.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);

                        foreach (string autor in autores)
                        {
                            string sujetoAutor = "<http://gnoss/" + filaDocumento.DocumentoID.ToString().ToUpper() + "> ";
                            string predicadoAutor = "<http://gnoss/hasautor> ";
                            string objetoAutor = "\"" + autor + "\" .";

                            mTripletasPerfilOrganizacion.Append(FacetadoAD.GenerarTripleta(sujetoAutor, predicadoAutor, objetoAutor));
                        }
                    }
                    #endregion

                    #endregion

                    LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);

                    List<QueryTriples> listaInformacionExtraRecursosContribuciones = actualizacionFacetadoCN.ObtieneInformacionExtraRecursosContribucionesOrg(id.Value, new Guid(pp.Value.ToString()));

                    foreach (QueryTriples query in listaInformacionExtraRecursosContribuciones)
                    {
                        string objeto = query.Objeto;
                        if (!query.Predicado.Contains("type") && !query.Predicado.Contains("hasEstado"))
                        {
                            objeto = UtilidadesVirtuoso.PasarObjetoALower(objeto);
                        }
                        mTripletasPerfilOrganizacion.Append(FacetadoAD.GenerarTripleta(query.Sujeto, query.Predicado, objeto));
                    }

                    LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);

                    InsertaTripletasConModify_ControlCheckPoint(ObtenerPrioridadFila(pFila), pp.Value.ToString(), mTripletasPerfilOrganizacion.ToString() + ' ' + tripletasextra, listaIdsEliminar, "", "", false, loggingService, entityContext, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);


                    if (id.HasValue)
                    {
                        if (tripleSearch == null)
                        {
                            if (tripletasYaAgregadas)
                            {
                                valorSearch = utilidadesVirtuoso.LeerSearchDeVirtuoso(id.Value, proyID, mUrlIntragnoss) + valorSearch;
                                AccionesExternasProyecto accionExternaSearch = dataWrapperProyectoAccionesExternas.ListaAccionesExternasProyecto.FirstOrDefault(item => item.TipoAccion.Equals((short)TipoAccionExterna.GenerarSearch) && item.ProyectoID.Equals(proyID));
                                if (accionExternaSearch != null && pFila.Table.DataSet is BaseRecursosComunidadDS)
                                {
                                    loggingService.AgregarEntrada("Search: se llama a un api externo para generar el search");
                                    string search = CallWebMethods.CallGetApi(accionExternaSearch.URL, $"?id={id.Value}");
                                    loggingService.AgregarEntrada("Search: Fin de la llamada al api externo para general al search");
                                    valorSearch = $"{valorSearch} {search}";
                                }
                            }

                            tripleSearch = ObtenerTipleSearch(valorSearch, id.Value, proyID, pFila.Table.DataSet is BaseRecursosComunidadDS, entityContext, loggingService, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                        }

                        //InsertarTripletas_ControlCheckPoint(ObtenerPrioridadFila(pFila), pp.Value.ToString(), tripleSearch, ObtenerPrioridadFila(pFila), false, "ColaReplicacionMaster");
                        utilidadesVirtuoso.InsertarTriplesEdicionTagsCategoriasSearchRecurso(id.Value, proyID, tripleSearch, mUrlIntragnoss, (PrioridadBase)(short)pFila["Prioridad"], false, false);
                    }
                }
            }
            if (mTripletasPerfilPersonal.Length > 0)
            {
                Dictionary<Guid, Guid> DicIDUS = new Dictionary<Guid, Guid>();
                DocumentacionCN docCN = new DocumentacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

                bool esBorrador = false;

                if (pListaDocsBorradores != null && pListaDocsBorradores.ContainsKey(id.Value))
                {
                    esBorrador = pListaDocsBorradores[id.Value];
                }
                else
                {
                    esBorrador = docCN.EsDocumentoBorrador(id.Value);
                }

                if (esBorrador)
                {
                    if (!listaIdsEliminar.ContainsKey(id.Value.ToString()))
                    {
                        listaIdsEliminar.Add(id.Value.ToString(), "");
                    }
                    string sujeto = "<http://gnoss/" + id + ">";
                    string predicadoprivacidad = "<http://gnoss/hasprivacidadMyGnoss> ";
                    string privacidad = "\"publico\" .";
                    mTripletasPerfilPersonal.Append(FacetadoAD.GenerarTripleta(sujeto, predicadoprivacidad, privacidad));
                }

                // Devuelve a todos aquellos que los tengan en su BR
                DicIDUS = docCN.ObtenerIdentidadyUsuarioIDdeRecurso(id.Value);
                foreach (KeyValuePair<Guid, Guid> pp in DicIDUS)
                {
                    #region privacidad my gnoss

                    //Obtenemos la privacidad de la persona
                    PersonaCN personaCN = new PersonaCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                    DataWrapperPersona dataWrapperPersona = personaCN.ObtenerPersonaPorUsuario(pp.Value);
                    dataWrapperPersona.ListaConfigGnossPersona.Add(personaCN.ObtenerConfiguracionPersonaPorID(dataWrapperPersona.ListaPersona.First().PersonaID));

                    string publico = "\"privado\" .";

                    if (dataWrapperPersona.ListaConfigGnossPersona.First().VerRecursosExterno)
                    {
                        publico = "\"publico\" .";
                    }
                    else if (dataWrapperPersona.ListaConfigGnossPersona.First().VerRecursos)
                    {
                        publico = "\"publicoreg\" .";
                    }

                    //Obtenemos las relaciones con el tesauro del documento
                    DataWrapperDocumentacion docDW = docCN.ObtenerDocumentoDocumentoWebVinBRPorIDDeUsuario(id.Value, pp.Value);
                    Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.Documento filaDocumento = docDW.ListaDocumento.FirstOrDefault(documento => documento.DocumentoID.Equals(id.Value));

                    bool privado = true;

                    if (docDW.ListaDocumentoWebAgCatTesauro.Count > 0)
                    {
                        Guid tesauroID = docDW.ListaDocumentoWebAgCatTesauro.First().TesauroID;
                        List<Guid> categoriasAgregadas = new List<Guid>();

                        //Obtenemos el tesauro
                        TesauroCN tesCN = new TesauroCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                        DataWrapperTesauro tesDW = tesCN.ObtenerTesauroCompletoPorID(tesauroID);

                        tesDW.Merge(tesCN.ObtenerTesauroUsuario(pp.Value));


                        tesCN.Dispose();


                        foreach (Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.DocumentoWebAgCatTesauro filaDocAgCat in docDW.ListaDocumentoWebAgCatTesauro)
                        {
                            if (!categoriasAgregadas.Contains(filaDocAgCat.CategoriaTesauroID))
                            {
                                categoriasAgregadas.Add(filaDocAgCat.CategoriaTesauroID);

                                //while padre
                                List<Es.Riam.Gnoss.AD.EntityModel.Models.Tesauro.CatTesauroAgCatTesauro> filasCatAgCat = tesDW.ListaCatTesauroAgCatTesauro.Where(item => item.CategoriaInferiorID.Equals(filaDocAgCat.CategoriaTesauroID)).ToList();
                                while (filasCatAgCat.Count > 0)
                                {
                                    Guid catPadreID = filasCatAgCat[0].CategoriaSuperiorID;
                                    if (!categoriasAgregadas.Contains(catPadreID))
                                    {
                                        categoriasAgregadas.Add(catPadreID);
                                    }
                                    filasCatAgCat = tesDW.ListaCatTesauroAgCatTesauro.Where(item => item.CategoriaInferiorID.Equals(catPadreID)).ToList();
                                }
                            }
                        }

                        //Inserto las filas de las relaciones con categorias:
                        string sujeto = "<http://gnoss/" + id.ToString().ToUpper() + ">";
                        string predicado = "<http://www.w3.org/2004/02/skos/core#ConceptID>";
                        foreach (Guid categoriaID in categoriasAgregadas)
                        {
                            string objeto = "<http://gnoss/" + categoriaID.ToString().ToUpper() + "> .";

                            mTripletasPerfilPersonal.Append(FacetadoAD.GenerarTripleta(sujeto, predicado, objeto));
                            mTripletasPerfilPersonal.Append(FacetadoAD.GenerarTripleta("<http://gnoss/" + categoriaID.ToString().ToUpper() + "> ", "<http://rdfs.org/sioc/ns#has_space>", "<http://gnoss/" + ProyectoAD.MetaProyecto.ToString() + "> ."));

                        }

                        //Inserto la privacidad del recurso
                        if (filaDocumento.UltimaVersion && !filaDocumento.Borrador && !filaDocumento.Eliminado)
                        {
                            foreach (Guid categoriaID in categoriasAgregadas)
                            {
                                if (tesDW.ListaTesauroUsuario.Count > 0 && categoriaID == tesDW.ListaTesauroUsuario.First().CategoriaTesauroPublicoID)
                                {
                                    privado = false;
                                }
                            }
                        }

                        if (filaDocumento.Borrador)
                        {
                            privado = true;
                        }

                        string privacidad = "\"privado\" .";
                        if (!privado)
                        {
                            privacidad = publico;
                        }

                        string predicadoprivacidad = "<http://gnoss/hasprivacidadMyGnoss> ";
                        mTripletasPerfilPersonal.Append(FacetadoAD.GenerarTripleta(sujeto, predicadoprivacidad, privacidad));
                    }
                    #region autores(aparte del check)

                    if (filaDocumento.Autor != null)
                    {
                        string[] autores = filaDocumento.Autor.Split(new string[] { "," }, StringSplitOptions.RemoveEmptyEntries);

                        foreach (string autor in autores)
                        {
                            string sujetoAutor = "<http://gnoss/" + filaDocumento.DocumentoID.ToString().ToUpper() + "> ";
                            string predicadoAutor = "<http://gnoss/hasautor> ";
                            string objetoAutor = "\"" + autor + "\" .";

                            mTripletasPerfilPersonal.Append(FacetadoAD.GenerarTripleta(sujetoAutor, predicadoAutor, objetoAutor));
                        }
                    }
                    #endregion

                    #endregion

                    LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);
                    //Obtengo el perfil a partir de la identidad
                    List<Guid> listaIdentidad = new List<Guid>();
                    listaIdentidad.Add(new Guid(pp.Key.ToString()));
                    List<Guid> listaPerfil = new List<Guid>();
                    IdentidadCN idenCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                    listaPerfil = idenCN.ObtenerPerfilesDeIdentidades(listaIdentidad);
                    string perfilamodificar = listaPerfil[0].ToString();

                    List<QueryTriples> listaInformacionExtraRecursos = actualizacionFacetadoCN.ObtieneInformacionExtraRecursosContribucionesPer(id.Value, new Guid(perfilamodificar));

                    //Si no se hace esto, se agrega 2 veces la fecha de la publicación.
                    string tripletasPersonalesTemporales = "";
                    foreach (QueryTriples query in listaInformacionExtraRecursos)
                    {
                        string objeto = query.Objeto;
                        if (!query.Predicado.Contains("type") && !query.Predicado.Contains("hasEstado"))
                        {
                            objeto = UtilidadesVirtuoso.PasarObjetoALower(objeto);
                        }
                        tripletasPersonalesTemporales += FacetadoAD.GenerarTripleta(query.Sujeto, query.Predicado, objeto);
                    }

                    LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);

                    InsertaTripletasConModify_ControlCheckPoint(ObtenerPrioridadFila(pFila), perfilamodificar, mTripletasPerfilPersonal.ToString() + ' ' + tripletasPersonalesTemporales, listaIdsEliminar, "", "", false, loggingService, entityContext, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);

                    if (id.HasValue)
                    {
                        if (tripleSearch == null)
                        {
                            if (tripletasYaAgregadas)
                            {
                                valorSearch = utilidadesVirtuoso.LeerSearchDeVirtuoso(id.Value, proyID, mUrlIntragnoss) + valorSearch;
                                AccionesExternasProyecto accionExternaSearch = dataWrapperProyectoAccionesExternas.ListaAccionesExternasProyecto.FirstOrDefault(item => item.TipoAccion.Equals((short)TipoAccionExterna.GenerarSearch) && item.ProyectoID.Equals(proyID));
                                if (accionExternaSearch != null && pFila.Table.DataSet is BaseRecursosComunidadDS)
                                {
                                    loggingService.AgregarEntrada("Search: se llama a un api externo para generar el search");
                                    string search = CallWebMethods.CallGetApi(accionExternaSearch.URL, $"?id={id.Value}");
                                    loggingService.AgregarEntrada("Search: Fin de la llamada al api externo para general al search");
                                    valorSearch = $"{valorSearch} {search}";
                                }
                            }

                            tripleSearch = ObtenerTipleSearch(valorSearch, id.Value, proyID, pFila.Table.DataSet is BaseRecursosComunidadDS, entityContext, loggingService, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                        }

                        //InsertarTripletas_ControlCheckPoint(ObtenerPrioridadFila(pFila), perfilamodificar, tripleSearch, ObtenerPrioridadFila(pFila), false, "ColaReplicacionMaster");
                        utilidadesVirtuoso.InsertarTriplesEdicionTagsCategoriasSearchRecurso(id.Value, proyID, tripleSearch, mUrlIntragnoss, (PrioridadBase)(short)pFila["Prioridad"], false, false);
                    }
                }
            }

            if (mTripletasContribuciones.Length > 0 && ((mTripletasPerfilPersonal.Length == 0 && mTripletasPerfilOrganizacion.Length == 0) || !proyID.Equals(ProyectoAD.MetaProyecto)) && generarGrafoContribuciones)
            {
                if (!listaIdsEliminar.ContainsKey(id.Value.ToString()))
                {
                    listaIdsEliminar.Add(id.Value.ToString(), "");
                }

                DocumentacionCN docCN = new DocumentacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                Guid identidadcreador = Guid.Empty;
                string comentarioorecurso = " ";
                if (listaTagsFiltros[(short)TiposTags.ComentarioORecurso].Count > 0)
                {
                    comentarioorecurso = listaTagsFiltros[(short)TiposTags.ComentarioORecurso][0];
                }
                if (comentarioorecurso.Contains("c"))
                { identidadcreador = docCN.ObtenerPublicadorAPartirIDsComentario(id.Value); }
                else
                {
                    if (docCN.EsDocumentoBorrador(id.Value))
                    {
                        if (!listaIdsEliminar.ContainsKey(id.Value.ToString()))
                        {
                            listaIdsEliminar.Add(id.Value.ToString(), "");
                        }
                    }

                    identidadcreador = docCN.ObtenerPublicadorAPartirIDsRecursoYProyecto(proyID, id.Value);
                }
                IdentidadCN idenCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

                List<Guid> resultado2 = idenCN.ObtenerPerfilyOrganizacionID(identidadcreador);

                if (id.HasValue)
                {
                    //Comprobar si el proyecto es de la tabla ProyectoSinRegistroPrevio
                    bool estaEnTablaSinRegistroObligatorio = false;
                    estaEnTablaSinRegistroObligatorio = EstaEnListaSinRegistroObligatorio(proyID, mListadeIDsProyectoSinRegistroObligatorio);
                    if (!estaEnTablaSinRegistroObligatorio)
                    {
                        if (tripleSearch == null)
                        {
                            if (tripletasYaAgregadas)
                            {
                                valorSearch = utilidadesVirtuoso.LeerSearchDeVirtuoso(id.Value, proyID, mUrlIntragnoss) + valorSearch;
                                AccionesExternasProyecto accionExternaSearch = dataWrapperProyectoAccionesExternas.ListaAccionesExternasProyecto.FirstOrDefault(item => item.TipoAccion.Equals((short)TipoAccionExterna.GenerarSearch) && item.ProyectoID.Equals(proyID));
                                if (accionExternaSearch != null && pFila.Table.DataSet is BaseRecursosComunidadDS)
                                {
                                    loggingService.AgregarEntrada("Search: se llama a un api externo para generar el search");
                                    string search = CallWebMethods.CallGetApi(accionExternaSearch.URL, $"?id={id.Value}");
                                    loggingService.AgregarEntrada("Search: Fin de la llamada al api externo para general al search");
                                    valorSearch = $"{valorSearch} {search}";
                                }
                            }

                            tripleSearch = ObtenerTipleSearch(valorSearch, id.Value, proyID, pFila.Table.DataSet is BaseRecursosComunidadDS, entityContext, loggingService, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                        }

                        mTripletasContribuciones.Append(" " + tripleSearch);

                        if (resultado2.Count > 1 && !resultado2[1].Equals(Guid.Empty))
                        {
                            Guid orgID = new Guid(resultado2[1].ToString());
                            List<QueryTriples> listaInformacionExtraRecursosContribuciones = actualizacionFacetadoCN.ObtieneInformacionExtraRecursosContribucionesOrg(id.Value, orgID);
                            foreach (QueryTriples query in listaInformacionExtraRecursosContribuciones)
                            {
                                string objeto = query.Objeto;
                                if (!query.Predicado.Contains("type") && !query.Predicado.Contains("hasEstado"))
                                {
                                    objeto = UtilidadesVirtuoso.PasarObjetoALower(objeto);
                                }
                                mTripletasContribuciones.Append(FacetadoAD.GenerarTripleta(query.Sujeto, query.Predicado, objeto));
                            }
                            LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);
                            InsertaTripletasConModify_ControlCheckPoint(ObtenerPrioridadFila(pFila), orgID.ToString(), mTripletasContribuciones.ToString(), listaIdsEliminar, "", "", false, loggingService, entityContext, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                        }
                        else if (resultado2.Count > 0)
                        {
                            Guid perfilID = new Guid(resultado2[0].ToString());
                            List<QueryTriples> listaInformacionExtraRecurso = actualizacionFacetadoCN.ObtieneInformacionExtraRecursosContribucionesPer(id.Value, perfilID);
                            foreach (QueryTriples query in listaInformacionExtraRecurso)
                            {
                                string objeto = query.Objeto;
                                if (!query.Predicado.Contains("type") && !query.Predicado.Contains("hasEstado"))
                                {
                                    objeto = UtilidadesVirtuoso.PasarObjetoALower(objeto);
                                }
                                mTripletasContribuciones.Append(FacetadoAD.GenerarTripleta(query.Sujeto, query.Predicado, objeto));
                            }
                            LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);
                            InsertaTripletasConModify_ControlCheckPoint(ObtenerPrioridadFila(pFila), perfilID.ToString(), mTripletasContribuciones.ToString(), listaIdsEliminar, "", "", false, loggingService, entityContext, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                        }
                    }
                }

                mTripletasContribuciones.Clear();
            }

            listaIdsEliminar.Clear();

            #endregion

            if ((int)pFila["TablaBaseProyectoID"] != 0)
            {
                Proyecto filaProy = null;
                if (filaProyecto != null)
                {
                    filaProy = filaProyecto;
                }
                else
                {
                    filaProy = proyectoCN.ObtenerProyectoPorID(proyID).ListaProyecto.FirstOrDefault();
                }

                ComparticionAutomaticaCN comparticionAutomaticaCN = new ComparticionAutomaticaCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                DataWrapperComparticionAutomatica comparticionAutomaticaDW = comparticionAutomaticaCN.ObtenerComparticionProyectoPorProyectoID(filaProy.OrganizacionID, filaProy.ProyectoID, false);

                if (comparticionAutomaticaDW.ListaComparticionAutomatica.Count > 0)
                {
                    //Insertar en la cola
                    BaseComunidadDS baseComunidadDS = new BaseComunidadDS();
                    BaseComunidadDS.ColaComparticionAutomaticaRow filaComparticionAutomatica = baseComunidadDS.ColaComparticionAutomatica.NewColaComparticionAutomaticaRow();

                    filaComparticionAutomatica.OrdenEjecucion = Guid.NewGuid();
                    filaComparticionAutomatica.ID = id.Value;
                    filaComparticionAutomatica.Tipo = (short)TiposEventoComparticion.RecursoAgregado;
                    filaComparticionAutomatica.Estado = 0;
                    filaComparticionAutomatica.Fecha = DateTime.Now;
                    filaComparticionAutomatica.Prioridad = (short)pFila["Prioridad"];

                    baseComunidadDS.ColaComparticionAutomatica.AddColaComparticionAutomaticaRow(filaComparticionAutomatica);

                    BaseComunidadCN baseComunidadCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                    baseComunidadCN.ActualizarBD(baseComunidadDS);
                }
            }
        }

        private bool EstaEnListaSinRegistroObligatorio(Guid pProyectoID, List<Guid> pListaProyectoSinRegistroObligatorio)
        {
            bool estaEnLista = false;
            foreach (Guid proyectoIDSinRegistroObligatorio in pListaProyectoSinRegistroObligatorio)
            {
                if (pProyectoID.Equals(proyectoIDSinRegistroObligatorio))
                {
                    return true;
                }

            }
            return estaEnLista;
        }


        /// <summary>
        /// Obtiene el triple de search de un recurso.
        /// </summary>
        /// <param name="pValorSearch">Valor anterior del search</param>
        /// <param name="pID">ID del elemento</param>
        /// <param name="pProyectoID">ID del proyeto</param>
        /// <param name="pEsRecurso">Verdad si es un recurso</param>
        /// <returns>Triple de search de un recurso</returns>
        public string ObtenerTipleSearch(string pValorSearch, Guid pID, Guid pProyectoID, bool pEsRecurso, EntityContext entityContext, LoggingService loggingService, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ParametroAplicacionCN parametroAplicacionCN = new ParametroAplicacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            string valor = parametroAplicacionCN.ObtenerParametroBusquedaPorTextoLibrePersonalizado();
            parametroAplicacionCN.Dispose();

            if (pEsRecurso && !string.IsNullOrEmpty(valor) && valor.Equals("1"))
            {
                return null;
            }
            else
            {
                try
                {
                    pValorSearch = $"{pValorSearch} {ObtenerValoresSemanticosSearch_ControlCheckPoint(mFicheroConfiguracionBD, mUrlIntragnoss, pProyectoID, pID, utilidadesVirtuoso)}";
                }
                catch (Exception ex)
                {
                    GuardarLog(ex, "Error al generar el SEARCH", loggingService);
                }

                pValorSearch = UtilCadenas.EliminarHtmlDeTextoPorEspacios(pValorSearch).Replace("\\", "/");
                pValorSearch = "\" " + pValorSearch.Replace("\"", "'").Trim() + " \" .";
                pValorSearch = pValorSearch.ToLower();

                string tripleSearch = FacetadoAD.GenerarTripleta("<http://gnoss/" + pID.ToString().ToUpper() + ">", "<http://gnoss/search>", UtilidadesVirtuoso.RemoverSignosSearch(UtilidadesVirtuoso.RemoverSignosAcentos(pValorSearch)));
                return tripleSearch;
            }
        }

        public int ObtenerPrioridadFila(DataRow pFila)
        {
            int prioridad = -1;
            int.TryParse(pFila["Prioridad"].ToString(), out prioridad);
            return prioridad;
        }

        /// <summary>
        /// Obtiene si se trata de un ecosistema sin metaproyecto
        /// </summary>
        public bool EsEcosistemaSinMetaProyecto
        {
            get
            {
                if (!mEsEcosistemaSinMetaProyecto.HasValue)
                {
                    //mEsEcosistemaSinMetaProyecto = GestorParametroAplicacionDS.ParametroAplicacion.Select("Parametro = 'EcosistemaSinMetaProyecto'").Length > 0 && bool.Parse((string)ParametroAplicacionDS.ParametroAplicacion.Select("Parametro = 'EcosistemaSinMetaProyecto'")[0]["Valor"]);
                    List<ParametroAplicacion> parametrosAplicacionB = GestorParametroAplicacionDS.ParametroAplicacion.Where(parametroApp => parametroApp.Parametro.Equals("EcosistemaSinMetaProyecto")).ToList();
                    mEsEcosistemaSinMetaProyecto = parametrosAplicacionB.Count > 0 && bool.Parse(parametrosAplicacionB.FirstOrDefault().Valor);
                }
                return mEsEcosistemaSinMetaProyecto.Value;
            }
        }

        /// <summary>
        /// Obtiene el grafo en el que se debe insertar los proyectos para la metabusqueda si se trata de un ecosistema sin metaproyecto
        /// </summary>
        public string GrafoMetaBusquedaComunidades
        {
            get
            {
                if (mGrafoMetaBusquedaComunidades == null)
                {
                    mGrafoMetaBusquedaComunidades = string.Empty;
                    List<ParametroAplicacion> parametrosAplicacion = GestorParametroAplicacionDS.ParametroAplicacion.Where(parametroApp => parametroApp.Parametro.Equals("GrafoMetaBusquedaComunidades")).ToList();
                    //if (ParametroAplicacionDS.ParametroAplicacion.Select("Parametro = 'GrafoMetaBusquedaComunidades'").Length > 0)
                    if (parametrosAplicacion.Count > 0)
                    {
                        mGrafoMetaBusquedaComunidades = parametrosAplicacion[0].Valor;
                    }
                }
                return mGrafoMetaBusquedaComunidades;
            }
        }

        /// <summary>
        /// Obtiene el grafo en el que se debe insertar las personas y las organizaciones para la metabusqueda si se trata de un ecosistema sin metaproyecto
        /// </summary>
        public string GrafoMetaBusquedaPerYOrg
        {
            get
            {
                if (mGrafoMetaBusquedaPerYOrg == null)
                {

                    mGrafoMetaBusquedaPerYOrg = string.Empty;
                    List<ParametroAplicacion> parametrosAplicacion = GestorParametroAplicacionDS.ParametroAplicacion.Where(parametroApp => parametroApp.Parametro.Equals("GrafoMetaBusquedaPerYOrg")).ToList();
                    //if (ParametroAplicacionDS.ParametroAplicacion.Select("Parametro = 'GrafoMetaBusquedaPerYOrg'").Length > 0)
                    if (parametrosAplicacion.Count > 0)
                    {
                        mGrafoMetaBusquedaPerYOrg = parametrosAplicacion[0].Valor;
                    }
                }
                return mGrafoMetaBusquedaPerYOrg;
            }
        }

        /// <summary>
        /// Obtiene el grafo en el que se debe insertar los recursos para la metabusqueda si se trata de un ecosistema sin metaproyecto
        /// </summary>
        public string GrafoMetaBusquedaRecursos
        {
            get
            {
                if (mGrafoMetaBusquedaRecursos == null)
                {
                    mGrafoMetaBusquedaRecursos = string.Empty;
                    List<ParametroAplicacion> parametrosAplicacion = GestorParametroAplicacionDS.ParametroAplicacion.Where(parametroApp => parametroApp.Parametro.Equals("GrafoMetaBusquedaRecursos")).ToList();
                    //if (ParametroAplicacionDS.ParametroAplicacion.Select("Parametro = 'GrafoMetaBusquedaRecursos'").Length > 0)
                    if (parametrosAplicacion.Count > 0)
                    {
                        mGrafoMetaBusquedaRecursos = parametrosAplicacion[0].Valor;
                    }
                }
                return mGrafoMetaBusquedaRecursos;
            }
        }

        #region Métodos para ProcesarFilaDeColaDeTipoAgregado por tipo

        protected Guid? ProcesarFilaDeColaDeTipoAgregadoRecursos(DataRow pFila, Dictionary<short, List<string>> listaTagsFiltros, ref Dictionary<string, string> listaIdsEliminar, ref DataWrapperFacetas tConfiguracion, ActualizacionFacetadoCN actualizacionFacetadoCN, Guid proyID, ref string valorSearch, ref List<string> tags, ref bool agregarTagsAModeloBase, bool TieneComponenteConCaducidadTipoRecurso, Proyecto filaProyecto, Guid organizacionID, List<string> listaTablasMantenerConfiguracion, Dictionary<Guid, bool> pListaDocsBorradores, List<string> listaTags, string pTitulo, string pDescripcion, Dictionary<Guid, Dictionary<Guid, FacetaDS>> pDiccionarioProyectoDocInformacionComunRecurso, ref bool pEsDocSemantico, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, EntityContextBASE entityContextBASE, VirtuosoAD virtuosoAD, GnossCache gnossCache, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetaDS facetaDS = new FacetaDS();
            Guid? ElementoID = null;
            bool tripletasYaAgregadas = pFila["Tipo"].Equals((short)TiposElementosEnCola.InsertadoEnGrafoBusquedaDesdeWeb);
            bool agregarSearch = pFila["Tipo"].Equals((short)TiposElementosEnCola.InsertadoEnGrafoBusquedaDesdeWeb);
            if (!(pFila is BaseRecursosComunidadDS.ColaTagsMyGnossRow))
            {
                List<QueryTriples> listaInformacionExtraComentariosContribuciones;
                string urlServicioArchivos = mConfigService.ObtenerUrlServicio("urlArchivos");

                #region Recursos
                ElementoID = new Guid(listaTagsFiltros[(short)TiposTags.IDTagDoc][0]);
                string idRecursoMay = ElementoID.ToString().ToUpper();
                string idRecursoMin = idRecursoMay.ToLower();
                listaIdsEliminar.Add(idRecursoMay, "rdf:type");
                DocumentacionCN docCN = new DocumentacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                string comentarioorecurso = " ";
                if (listaTagsFiltros[(short)TiposTags.ComentarioORecurso].Count > 0)
                {
                    comentarioorecurso = listaTagsFiltros[(short)TiposTags.ComentarioORecurso][0];
                }
                if (comentarioorecurso.Contains("c") || comentarioorecurso.Contains("f"))
                {
                    listaInformacionExtraComentariosContribuciones = actualizacionFacetadoCN.ObtieneInformacionExtraComentariosContribuciones(ElementoID.Value);
                    foreach (QueryTriples query in listaInformacionExtraComentariosContribuciones)
                    {
                        string objeto = query.Objeto;
                        if (!query.Predicado.Contains("type") && !query.Predicado.Contains("hasEstado"))
                        {
                            objeto = UtilidadesVirtuoso.PasarObjetoALower(objeto);
                        }
                        mTripletasContribuciones.Append(FacetadoAD.GenerarTripleta(query.Sujeto.Replace(idRecursoMin, idRecursoMay), query.Predicado, objeto));
                    }
                    LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);
                    ActualizarNumComentariosVirtuoso(ElementoID.Value, entityContext, loggingService, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                }               
                else
                {
                    bool esborrador = false;
                    if (pListaDocsBorradores != null && pListaDocsBorradores.ContainsKey(ElementoID.Value))
                    {
                        esborrador = pListaDocsBorradores[ElementoID.Value];
                    }
                    else
                    {
                        esborrador = docCN.EsDocumentoBorrador(ElementoID.Value);
                    }
                    if (proyID.Equals(ProyectoAD.MetaProyecto))
                    {
                        OrganizacionCN organizacionCN = new OrganizacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                        //Si es perfil de organizacion
                        List<QueryTriples> listaTriplesOrganizacion = actualizacionFacetadoCN.ObtieneInformacionGeneralRecursoOrganizacion(ElementoID.Value);
                        foreach (QueryTriples query in listaTriplesOrganizacion)
                        {
                            string objeto = query.Objeto;

                            if (query.Predicado.Contains("hastipodoc") && objeto.Contains("19"))
                            { objeto = objeto.Replace("19", "2"); }
                            if (query.Predicado.Contains("hastipodoc") && objeto.Contains("20"))
                            { objeto = objeto.Replace("20", "21"); }
                            if (query.Predicado.Contains("hastipodoc") && objeto.Contains("24"))
                            { objeto = objeto.Replace("24", "2"); }
                            if (query.Predicado.Contains("hastipodoc") && objeto.Contains("25"))
                            { objeto = objeto.Replace("25", "21"); }

                            if (!query.Predicado.Contains("type") && !query.Predicado.Contains("hasEstadoPP") && !query.Predicado.Contains("hasOrigen"))
                            {
                                objeto = UtilidadesVirtuoso.PasarObjetoALower(objeto);
                            }
                            mTripletasPerfilOrganizacion.Append(FacetadoAD.GenerarTripleta(query.Sujeto.Replace(idRecursoMin, idRecursoMay), query.Predicado, objeto));
                        }
                        LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);

                        //Si es perfil personal
                        List<QueryTriples> listaTriplesPersonal = actualizacionFacetadoCN.ObtieneInformacionGeneralRecursoPersonal(ElementoID.Value);
                        foreach (QueryTriples query in listaTriplesPersonal)
                        {
                            string objeto = query.Objeto;

                            if (query.Predicado.Contains("hastipodoc") && objeto.Contains("19"))
                            { objeto = objeto.Replace("19", "2"); }
                            if (query.Predicado.Contains("hastipodoc") && objeto.Contains("20"))
                            { objeto = objeto.Replace("20", "21"); }
                            if (query.Predicado.Contains("hastipodoc") && objeto.Contains("24"))
                            { objeto = objeto.Replace("24", "2"); }
                            if (query.Predicado.Contains("hastipodoc") && objeto.Contains("25"))
                            { objeto = objeto.Replace("25", "21"); }

                            if (!query.Predicado.Contains("type") && !query.Predicado.Contains("hasEstadoPP") && !query.Predicado.Contains("hasOrigen"))
                            {
                                objeto = UtilidadesVirtuoso.PasarObjetoALower(objeto);
                            }

                            mTripletasPerfilPersonal.Append(FacetadoAD.GenerarTripleta(query.Sujeto.Replace(idRecursoMin, idRecursoMay), query.Predicado, objeto));
                        }
                        LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);
                    }
                    if (facetaDS.Tables["TripletasExtraContribucionesRecurso"] == null || facetaDS.Tables["TripletasExtraContribucionesRecurso"].Select($"Column1='<http://gnoss/{idRecursoMay}>'").Length == 0)
                    {
                        //MJ Contribuciones obtener perfil o organizacion
                        mTripletasContribuciones.Append(actualizacionFacetadoCN.ObtieneInformacionExtraRecursosContribuciones(ElementoID.Value, docCN.ObtenerPerfilPublicadorDocumento(ElementoID.Value, filaProyecto.ProyectoID), proyID));
                    }

                    if (listaTags == null)
                    {
                        listaTags = actualizacionFacetadoCN.ObtenerTags(ElementoID.Value, "Recurso", proyID);
                    }
                    foreach (string tag in listaTags)
                    {
                        string objeto = $"\"{tag.Replace("\"", "'").Trim()}\" .";
                        mTripletasContribuciones.Append(FacetadoAD.GenerarTripleta($"<http://gnoss/{idRecursoMay}>", "<http://rdfs.org/sioc/types#Tag>", objeto));
                        if (proyID.Equals(ProyectoAD.MetaProyecto))
                        {
                            mTripletasPerfilPersonal.Append(FacetadoAD.GenerarTripleta($"<http://gnoss/{idRecursoMay}>", "<http://rdfs.org/sioc/types#Tag>", objeto));
                        }
                    }

                    LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);

                    if (!esborrador)
                    {
                        //tag descompuestos titulo
                        if (string.IsNullOrEmpty(pTitulo))
                        {
                            pTitulo = actualizacionFacetadoCN.ObtenerTituloRecurso(ElementoID.Value);
                        }
                        string titulo = UtilCadenas.EliminarHtmlDeTexto(pTitulo);


                        Dictionary<string, string> idiomaTitulo = UtilCadenas.ObtenerTextoPorIdiomas(titulo);

                        string tripleFoafFirstName = "";

                        if (idiomaTitulo.Count > 0)
                        {
                            List<string> idiomasSimilares = new List<string>();

                            foreach (string idioma in idiomaTitulo.Keys)
                            {
                                if (!tripletasYaAgregadas)
                                {
                                    mTripletas.Append(FacetadoAD.GenerarTripleta($"<http://gnoss/{idRecursoMay}>", "<http://gnoss/hasnombrecompleto>", $"\"{UtilidadesVirtuoso.PasarObjetoALower(idiomaTitulo[idioma])}\"", idioma));

                                    mTripletas.Append(UtilidadesVirtuoso.AgregarTripletaDesnormalizadaTitulo(ElementoID.Value, UtilidadesVirtuoso.RemoverSignosSearch(UtilidadesVirtuoso.RemoverSignosAcentos(UtilidadesVirtuoso.PasarObjetoALower(idiomaTitulo[idioma])))));
                                }

                                tripleFoafFirstName += FacetadoAD.GenerarTripleta($"<http://gnoss/{idRecursoMay}>", "<http://xmlns.com/foaf/0.1/firstName>", $"\"{UtilidadesVirtuoso.PasarObjetoALower(idiomaTitulo[idioma])}\"", idioma);

                                if (!idiomasSimilares.Contains(idiomaTitulo[idioma]))
                                {
                                    AgregarTripletasDescompuestas(ElementoID.Value, proyID, "<http://gnoss/hasTagTituloDesc>", idiomaTitulo[idioma], true, true, listaTags, tripletasYaAgregadas);

                                    valorSearch += $" {idiomaTitulo[idioma]}";

                                    idiomasSimilares.Add(idiomaTitulo[idioma]);
                                }
                            }
                        }
                        else
                        {
                            AgregarTripletasDescompuestas(ElementoID.Value, proyID, "<http://gnoss/hasTagTituloDesc>", titulo, true, true, listaTags, tripletasYaAgregadas);

                            if (!tripletasYaAgregadas)
                            {
                                mTripletas.Append(FacetadoAD.GenerarTripleta($"<http://gnoss/{idRecursoMay}>", "<http://gnoss/hasnombrecompleto>", $"\"{UtilidadesVirtuoso.PasarObjetoALower(titulo)}\""));

                                mTripletas.Append(UtilidadesVirtuoso.AgregarTripletaDesnormalizadaTitulo(ElementoID.Value, UtilidadesVirtuoso.RemoverSignosSearch(UtilidadesVirtuoso.RemoverSignosAcentos(UtilidadesVirtuoso.PasarObjetoALower(titulo)))));

                                tripleFoafFirstName = FacetadoAD.GenerarTripleta($"<http://gnoss/{idRecursoMay}>", "<http://xmlns.com/foaf/0.1/firstName>", $"\"{UtilidadesVirtuoso.PasarObjetoALower(titulo)}\"");
                            }

                            valorSearch += $" {pTitulo}";
                        }

                        pDescripcion = actualizacionFacetadoCN.ObtenerDescripcionRecurso(ElementoID.Value);

                        if (pDescripcion != null && !string.IsNullOrEmpty(pDescripcion))
                        {
                            Dictionary<string, string> idiomaDescp = UtilCadenas.ObtenerTextoPorIdiomas(pDescripcion);

                            if (idiomaDescp.Count > 0)
                            {
                                List<string> idiomasSimilares = new List<string>();

                                foreach (string idioma in idiomaDescp.Keys)
                                {
                                    if (!idiomasSimilares.Contains(idiomaDescp[idioma]))
                                    {
                                        valorSearch += $" {idiomaDescp[idioma]}";
                                        idiomasSimilares.Add(idiomaDescp[idioma]);
                                    }
                                }
                            }
                            else
                            {
                                valorSearch += " " + pDescripcion;
                            }
                        }

                        //tags etiquetas descompuestos
                        foreach (string tag in listaTags)
                        {
                            string objeto = $"\"{tag.Replace("\"", "'").Trim()}\" .";

                            if (!tripletasYaAgregadas)
                            {
                                mTripletas.Append(FacetadoAD.GenerarTripleta($"<http://gnoss/{idRecursoMay}>", "<http://rdfs.org/sioc/types#Tag>", objeto));
                            }
                            tags.Add(tag);
                        }

                        mTripletas.Append(ObtenerTripletasCategoriasRecurso(ElementoID.Value, proyID, ref valorSearch, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication));

                        //Añado lo que no nos llegan los datos
                        List<QueryTriples> listaResultadosInformacionComunRecurso = new List<QueryTriples>();
                        if (pDiccionarioProyectoDocInformacionComunRecurso != null && pDiccionarioProyectoDocInformacionComunRecurso.ContainsKey(proyID) && pDiccionarioProyectoDocInformacionComunRecurso[proyID].ContainsKey(ElementoID.Value))
                        {
                            facetaDS.Merge(pDiccionarioProyectoDocInformacionComunRecurso[proyID][ElementoID.Value]);
                        }
                        else
                        {
                            listaResultadosInformacionComunRecurso.AddRange(actualizacionFacetadoCN.ObtieneInformacionComunRecurso(ElementoID.Value, proyID));
                        }
                        //obtengo categorias documento
                        DataWrapperDocumentacion docDW = docCN.ObtenerDocumentoDocumentoWebVinBRPorID(ElementoID.Value, proyID);
                        docCN.Dispose();
                        Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.Documento filaDocumento = docDW.ListaDocumento.Where(doc => doc.DocumentoID.Equals(ElementoID.Value)).ToList().FirstOrDefault();

                        if (filaDocumento == null)
                        {
                            return Guid.Empty;
                        }

                        if (!tripletasYaAgregadas)
                        {
                            //añade la tripleta de comunidad origen si el recurso se ha compartido automáticamente
                            string tripleComparticionAutomatica = "";
                            Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.DocumentoWebVinBaseRecursos filaDocWebVinBR = null;

                            if (filaDocumento.DocumentoWebVinBaseRecursos.Count > 0)
                            {
                                filaDocWebVinBR = filaDocumento.DocumentoWebVinBaseRecursos.First();

                                if (filaDocWebVinBR != null && filaDocWebVinBR.TipoPublicacion.Equals((short)TipoPublicacion.CompartidoAutomatico))
                                {
                                    tripleComparticionAutomatica = FacetadoAD.GenerarTripleta($"<http://gnoss/{idRecursoMay}>", "<http://gnoss/hasComunidadOrigen>", $"<http://gnoss/{filaDocumento.ProyectoID.ToString().ToUpper()}>");

                                    mTripletas.Append(tripleComparticionAutomatica);
                                }
                            }
                        }
                        //Se ha añadido el recurso y no es borrador
                        #region ColaSiteMaps y ColaActualizarContextos

                        if (filaProyecto != null && filaProyecto.ProyectoID != ProyectoAD.MetaProyecto && docDW.ListaDocumentoWebVinBaseRecursos.Count > 0)
                        {
                            BaseComunidadCN baseComunidadCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                            ParametroGeneralCN paramGralCN = new ParametroGeneralCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                            ParametroGeneral filaParamGral = paramGralCN.ObtenerFilaParametrosGeneralesDeProyecto(filaProyecto.ProyectoID);
                            paramGralCN.Dispose();

                            //si existe el sitemap de la comunidad añado el recurso
                            if (filaParamGral.TieneSitemapComunidad)
                            {
                                //si las fechas son las mismas el documento es nuevo
                                if (filaDocumento.FechaCreacion.Equals(filaDocumento.FechaModificacion) && filaDocumento.FechaCreacion.HasValue)
                                {
                                    baseComunidadCN.InsertarFilaEnColaColaSitemaps(ElementoID.Value, TiposEventoSitemap.RecursoNuevo, 0, filaDocumento.FechaCreacion.Value, 1, filaProyecto.NombreCorto);
                                }
                                else if (filaDocumento.FechaModificacion.HasValue)//si no, el documento es editado
                                {
                                    baseComunidadCN.InsertarFilaEnColaColaSitemaps(ElementoID.Value, TiposEventoSitemap.RecursoModificado, 0, filaDocumento.FechaModificacion.Value, 1, filaProyecto.NombreCorto);
                                }
                            }
                            //se agrega la fila a la ColaActualizarContextos
                            //baseComunidadCN.InsertarFilaEnColaActualizaContextos(idRecurso, 0, (short)pFila["Prioridad"], DateTime.Now);
                            baseComunidadCN.Dispose();
                        }
                        #endregion
                        if (docDW.ListaDocumentoWebAgCatTesauro.Count > 0)
                        {
                            Guid tesauroID = docDW.ListaDocumentoWebAgCatTesauro.First().TesauroID;
                            List<Guid> categoriasAgregadas = new List<Guid>();
                            //obtener tesauro
                            TesauroCN tesCN = new TesauroCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                            DataWrapperTesauro tesDW = tesCN.ObtenerTesauroCompletoPorID(tesauroID);
                            tesCN.Dispose();
                            foreach (Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.DocumentoWebAgCatTesauro filaDocAgCat in docDW.ListaDocumentoWebAgCatTesauro)
                            {
                                if (!categoriasAgregadas.Contains(filaDocAgCat.CategoriaTesauroID))
                                {
                                    categoriasAgregadas.Add(filaDocAgCat.CategoriaTesauroID);

                                    //while padre           
                                    List<Es.Riam.Gnoss.AD.EntityModel.Models.Tesauro.CatTesauroAgCatTesauro> filasCatAgCat = tesDW.ListaCatTesauroAgCatTesauro.Where(catTesAg => catTesAg.CategoriaInferiorID.Equals(filaDocAgCat.CategoriaTesauroID)).ToList();
                                    while (filasCatAgCat.Count > 0)
                                    {
                                        Guid catPadreID = filasCatAgCat[0].CategoriaSuperiorID;
                                        if (!categoriasAgregadas.Contains(catPadreID))
                                        {
                                            categoriasAgregadas.Add(catPadreID);
                                        }
                                        filasCatAgCat = tesDW.ListaCatTesauroAgCatTesauro.Where(catTesAg => catTesAg.CategoriaInferiorID.Equals(catPadreID)).ToList();
                                    }
                                }
                            }

                            //Inserto las filas:
                            string sujeto = $"<http://gnoss/{idRecursoMay}>";
                            string predicado = "<http://www.w3.org/2004/02/skos/core#ConceptID>";
                            foreach (Guid categoriaID in categoriasAgregadas)
                            {
                                string objeto = $"<http://gnoss/{categoriaID.ToString().ToUpper()}> .";

                                if (!tripletasYaAgregadas)
                                {
                                    mTripletas.Append(FacetadoAD.GenerarTripleta(sujeto, predicado, objeto));
                                }

                                mTripletasContribuciones.Append(FacetadoAD.GenerarTripleta(sujeto, predicado, objeto));
                                mTripletasContribuciones.Append(FacetadoAD.GenerarTripleta(objeto.Replace(".", ""), "<http://rdfs.org/sioc/ns#has_space>", $"<http://gnoss/{proyID.ToString().ToUpper()}>  ."));
                            }

                            if (filaDocumento.Tipo != (short)TiposDocumentacion.Semantico)
                            {
                                FilaProyecto = filaProyecto;

                                if (!string.IsNullOrEmpty(UrlMappingCategorias(entityContext, loggingService, servicesUtilVirtuosoAndReplication)))
                                {
                                    //obtener categorias tesauro de ese recurso
                                    List<Guid> listaCategoriasRecurso = new List<Guid>();
                                    List<Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.DocumentoWebAgCatTesauro> filasDocWebAgCatTesauro = docDW.ListaDocumentoWebAgCatTesauro.Where(docWeb => docWeb.DocumentoID.Equals(ElementoID.Value)).ToList();

                                    foreach (Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.DocumentoWebAgCatTesauro fila in filasDocWebAgCatTesauro)
                                    {
                                        if (!listaCategoriasRecurso.Contains(fila.CategoriaTesauroID))
                                        {
                                            listaCategoriasRecurso.Add(fila.CategoriaTesauroID);
                                        }
                                    }

                                    Uri uriTest = null;

                                    //obtener las categorias semánticas del mapeo que corresponden a las anteriores
                                    if (string.IsNullOrEmpty(urlServicioArchivos))
                                    {
                                        string mensaje = "Excepción: el parametro urlServicioArchivos no está configurado en el config del servicio";
                                        this.GuardarLog("ERROR:  " + mensaje, loggingService);
                                        throw new Exception(mensaje);
                                    }
                                    else if (!Uri.TryCreate(urlServicioArchivos, UriKind.Absolute, out uriTest))
                                    {
                                        string mensaje = "Excepción: el parametro urlServicioArchivos está mal configurado en el config del servicio: " + urlServicioArchivos;
                                        this.GuardarLog("ERROR:  " + mensaje, loggingService);
                                        throw new Exception(mensaje);
                                    }

                                    //obtener el byte[] con el mapeo
                                    byte[] arrayMapeo = ObtenerMapeoTesauro(filaProyecto.ProyectoID, UrlMappingCategorias(entityContext, loggingService, servicesUtilVirtuosoAndReplication), mFicheroConfiguracionBD, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication);

                                    DocumentacionCL docCL = new DocumentacionCL(mFicheroConfiguracionBD, mFicheroConfiguracionBD, entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
                                    List<string> listaTriples = docCL.MapearCategoriasTesauroComunidad(listaCategoriasRecurso, filaProyecto.ProyectoID, UrlMappingCategorias(entityContext, loggingService, servicesUtilVirtuosoAndReplication), sujeto, arrayMapeo);
                                    docCL.Dispose();

                                    foreach (string triple in listaTriples)
                                    {
                                        if (!tripletasYaAgregadas)
                                        {
                                            mTripletas.Append(triple);
                                        }
                                        mTripletasContribuciones.Append(triple);
                                    }
                                }
                            }

                        }
                        if (filaDocumento.Autor != null)
                        {
                            if (!string.IsNullOrEmpty(filaDocumento.Autor))
                            {
                                char[] separadores = { ',' };
                                string[] autores = filaDocumento.Autor.Split(separadores, StringSplitOptions.RemoveEmptyEntries);

                                string sujeto = $"<http://gnoss/{filaDocumento.DocumentoID.ToString().ToUpper()}> ";
                                string predicado = "<http://gnoss/hasautor> ";

                                foreach (string autor in autores)
                                {
                                    string objeto = "\"" + autor.Replace("\"", "'").Trim() + "\" .";
                                    string tripletaAutor = FacetadoAD.GenerarTripleta(sujeto, predicado, objeto.ToLower());

                                    if (!tripletasYaAgregadas)
                                    {
                                        mTripletas.Append(tripletaAutor);
                                    }

                                    mTripletasContribuciones.Append(tripletaAutor);
                                    valorSearch += $" {autor} ";
                                }
                            }
                        }

                        //cargo informacionComunRecursosenComunidad
                        foreach (QueryTriples query in listaResultadosInformacionComunRecurso)
                        {
                            string objeto = query.Objeto;
                            //firstName se agrega más arriba en mTripletas.
                            if (!tripletasYaAgregadas && !esborrador && query.Predicado != "<http://xmlns.com/foaf/0.1/firstName>")
                            {
                                mTripletas.Append(FacetadoAD.GenerarTripleta(query.Sujeto.Replace(idRecursoMin, idRecursoMay), query.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));
                            }
                            mTripletasContribuciones.Append(FacetadoAD.GenerarTripleta(query.Sujeto.Replace(idRecursoMin, idRecursoMay), query.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));


                        }
                        LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);
                        //cargo informacionComunRecursosen Mygnoss
                        if (pDiccionarioProyectoDocInformacionComunRecurso != null && pDiccionarioProyectoDocInformacionComunRecurso.ContainsKey(ProyectoAD.MetaProyecto) && pDiccionarioProyectoDocInformacionComunRecurso[ProyectoAD.MetaProyecto].ContainsKey(ElementoID.Value))
                        {
                            facetaDS.Merge(pDiccionarioProyectoDocInformacionComunRecurso[ProyectoAD.MetaProyecto][ElementoID.Value]);
                        }
                        else
                        {
                            listaResultadosInformacionComunRecurso.AddRange(actualizacionFacetadoCN.ObtieneInformacionComunRecurso(ElementoID.Value, ProyectoAD.MetaProyecto));
                        }

                        LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);

                        foreach (short tipo in listaTagsFiltros.Keys)
                        {
                            if (tipo.Equals((short)TiposTags.TipoDocumento))
                            {
                                string tipoDoc = listaTagsFiltros[(short)TiposTags.TipoDocumento][0];

                                //Pregunta
                                if (tipoDoc.Contains("15"))
                                {
                                    if (!tripletasYaAgregadas)
                                    {
                                        mTripletas.Append(FacetadoAD.GenerarTripleta($"<http://gnoss/{idRecursoMay}>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "\"Pregunta\""));
                                    }

                                    AnyadirTripleFoafFirstName(mTripletas.ToString(), ElementoID.Value, tripleFoafFirstName);

                                    //es la miama en MyGnoss que una comunidad
                                    List<QueryTriples> informacionExtraPreguntas = actualizacionFacetadoCN.ObtieneInformacionExtraPregunta(ElementoID.Value, proyID);
                                    foreach (QueryTriples query in informacionExtraPreguntas)
                                    {
                                        string objeto = query.Objeto;
                                        if (!tripletasYaAgregadas)
                                        {
                                            mTripletas.Append(FacetadoAD.GenerarTripleta(query.Sujeto.Replace(idRecursoMin, idRecursoMay), query.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));
                                        }

                                        mTripletasContribuciones.Append(FacetadoAD.GenerarTripleta(query.Sujeto.Replace(idRecursoMin, idRecursoMay), query.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));
                                    }
                                    LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);
                                    #region borramos cache preguntas
                                    if ((short)pFila["Prioridad"] < 11 || (short)pFila["Prioridad"] > 20)
                                    {
                                        if (filaProyecto.NumeroPreguntas > 3000)
                                        {
                                            BaseComunidadCN baseComunidadCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                                            try
                                            {
                                                baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Preguntas, null);
                                            }
                                            catch (Exception ex)
                                            {
                                                loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                                                baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Preguntas);
                                            }

                                            baseComunidadCN.Dispose();
                                        }
                                        else
                                        {
                                            FacetadoCL facetadoCL = new FacetadoCL(mFicheroConfiguracionBD, mFicheroConfiguracionBD, mUrlIntragnoss, entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                                            facetadoCL.Dominio = mDominio;
                                            facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(proyID, FacetadoAD.TipoBusquedaToString(TipoBusqueda.Preguntas));
                                            facetadoCL.BorrarRSSDeComunidad(proyID);
                                            facetadoCL.Dispose();

                                            if (TieneComponenteConCaducidadTipoRecurso)
                                            {
                                                BaseComunidadCN baseComunidadCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                                                try
                                                {
                                                    baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.RefrescarComponentesRecursos, TipoBusqueda.Recursos, null);
                                                }
                                                catch (Exception ex)
                                                {
                                                    loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                                                    baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.RefrescarComponentesRecursos, TipoBusqueda.Recursos);
                                                }

                                                baseComunidadCN.Dispose();
                                            }
                                        }
                                    }
                                    #endregion
                                }//Debate
                                else if (tipoDoc.Contains("16"))
                                {
                                    if (!tripletasYaAgregadas)
                                    {
                                        mTripletas.Append(FacetadoAD.GenerarTripleta($"<http://gnoss/{idRecursoMay}>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "\"Debate\""));
                                    }
                                    AnyadirTripleFoafFirstName(mTripletas.ToString(), ElementoID.Value, tripleFoafFirstName);

                                    //es la misma en Mygnoss que en una comunidad
                                    List<QueryTriples> listaInformacionExtraDebate = actualizacionFacetadoCN.ObtieneInformacionExtraDebate(ElementoID.Value, proyID);
                                    foreach (QueryTriples query in listaInformacionExtraDebate)
                                    {
                                        string objeto = query.Objeto;
                                        if (!tripletasYaAgregadas)
                                        {
                                            mTripletas.Append(FacetadoAD.GenerarTripleta(query.Sujeto.Replace(idRecursoMin, idRecursoMay), query.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));
                                        }
                                        mTripletasContribuciones.Append(FacetadoAD.GenerarTripleta(query.Sujeto.Replace(idRecursoMin, idRecursoMay), query.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));
                                    }
                                    LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);

                                    #region borramos cache debates
                                    if ((short)pFila["Prioridad"] < 11 || (short)pFila["Prioridad"] > 20)
                                    {
                                        if (filaProyecto.NumeroDebates > 3000)
                                        {
                                            BaseComunidadCN baseComunidadCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                                            try
                                            {
                                                baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Debates, null);
                                            }
                                            catch (Exception ex)
                                            {
                                                loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                                                baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Debates);
                                            }
                                            baseComunidadCN.Dispose();
                                        }
                                        else
                                        {
                                            FacetadoCL facetadoCL = new FacetadoCL(mFicheroConfiguracionBD, mFicheroConfiguracionBD, mUrlIntragnoss, entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                                            facetadoCL.Dominio = mDominio;
                                            facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(proyID, FacetadoAD.TipoBusquedaToString(TipoBusqueda.Debates));
                                            facetadoCL.BorrarRSSDeComunidad(proyID);
                                            facetadoCL.Dispose();

                                            if (TieneComponenteConCaducidadTipoRecurso)
                                            {
                                                BaseComunidadCN baseComunidadCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                                                try
                                                {
                                                    baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.RefrescarComponentesRecursos, TipoBusqueda.Recursos, null);
                                                }
                                                catch (Exception ex)
                                                {
                                                    loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                                                    baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.RefrescarComponentesRecursos, TipoBusqueda.Recursos);
                                                }
                                                baseComunidadCN.Dispose();
                                            }
                                        }
                                    }
                                    #endregion
                                }//Encuesta
                                else if (tipoDoc.Contains("18"))
                                {
                                    if (!tripletasYaAgregadas)
                                    {
                                        mTripletas.Append(FacetadoAD.GenerarTripleta($"<http://gnoss/{idRecursoMay}>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "\"Encuesta\""));
                                    }

                                    AnyadirTripleFoafFirstName(mTripletas.ToString(), ElementoID.Value, tripleFoafFirstName);

                                    //es la misma en Mygnoss que en una comunidad
                                    List<QueryTriples> listaInformacionExtraEncuesta = actualizacionFacetadoCN.ObtieneInformacionExtraEncuesta(ElementoID.Value, proyID);
                                    foreach (QueryTriples query in listaInformacionExtraEncuesta)
                                    {
                                        string objeto = query.Objeto;
                                        if (!tripletasYaAgregadas)
                                        {
                                            mTripletas.Append(FacetadoAD.GenerarTripleta(query.Sujeto.Replace(idRecursoMin, idRecursoMay), query.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));
                                        }
                                        mTripletasContribuciones.Append(FacetadoAD.GenerarTripleta(query.Sujeto.Replace(idRecursoMin, idRecursoMay), query.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));
                                    }
                                    LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);
                                    #region borramos cache encuestas
                                    if ((short)pFila["Prioridad"] < 11 || (short)pFila["Prioridad"] > 20)
                                    {
                                        FacetadoCL facetadoCL = new FacetadoCL(mFicheroConfiguracionBD, mFicheroConfiguracionBD, mUrlIntragnoss, entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                                        facetadoCL.Dominio = mDominio;
                                        facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(proyID, FacetadoAD.TipoBusquedaToString(TipoBusqueda.Encuestas));
                                        facetadoCL.BorrarRSSDeComunidad(proyID);
                                        facetadoCL.Dispose();
                                    }
                                    #endregion
                                }
                                else
                                {
                                    string tempTipoDoc = tipoDoc.Replace(Constantes.TIPO_DOC, "");
                                    int tipoDocInt;
                                    string rdfType = "";
                                    if (int.TryParse(tempTipoDoc, out tipoDocInt))
                                    {
                                        ParametroGeneralCN paramGralCN = new ParametroGeneralCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                                        List<ProyectoRDFType> filaProyectoRdfType = paramGralCN.ObtenerProyectoRDFType(filaProyecto.ProyectoID, tipoDocInt);
                                        paramGralCN.Dispose();
                                        if (filaProyectoRdfType.Count > 0)
                                        {
                                            rdfType = filaProyectoRdfType[0].RdfType;
                                        }
                                    }
                                    string typeSem = "";
                                    if (tipoDoc.Contains("#5#"))
                                    {
                                        pEsDocSemantico = true;
                                        string triples = ObtenerTriplesFormularioSemantico_ControlCheckPoint(mFicheroConfiguracionBD, mFicheroConfiguracionBDBase, mUrlIntragnoss, tConfiguracion, organizacionID, proyID, ElementoID.Value, out typeSem, loggingService, entityContext, redisCacheWrapper, gnossCache, entityContextBASE, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                                        if (!tripletasYaAgregadas)
                                        {
                                            mTripletas.Append(triples);
                                        }
                                        //mTripletasGnoss.Append(triples);
                                        mTripletasContribuciones.Append(triples);

                                        Uri uriTest = null;
                                        bool uriValida = Uri.TryCreate(urlServicioArchivos, UriKind.Absolute, out uriTest);

                                        if (!uriValida)
                                        {
                                            string mensaje = $"Excepción: el parametro urlServicioArchivos está mal configurado en el config del servicio: {urlServicioArchivos}";
                                            mensaje += $"\r\n Url de configuracion: {mConfigService.ObtenerUrlServicio("urlArchivos")}";
                                            mensaje += $"\r\n UrlIntragnoss: {this.GestorParametroAplicacionDS.ParametroAplicacion.Find(parametroApp => parametroApp.Parametro.Equals("UrlIntragnoss")).Valor}";

                                            List<Es.Riam.Gnoss.AD.EntityModel.ConfiguracionServicios> filasConfigServicios = this.GestorParametroAplicacionDS.ListaConfiguracionServicios.Where(confServicios => confServicios.Nombre.Equals("urlServicioArchivos")).ToList();
                                            if (filasConfigServicios != null && filasConfigServicios.Count > 0)
                                            {
                                                mensaje += $"\r\n UrlServicioArchivos: {filasConfigServicios[0].NumServicio}: {filasConfigServicios[0].Url}";
                                            }
                                            GuardarLog($"ALERT: {mensaje}", loggingService);
                                        }

                                        Dictionary<Guid, List<MetaKeyword>> dicOntologiaMetas = new Dictionary<Guid, List<MetaKeyword>>();

                                        //se cargan las metaetiquetas del xml de la ontología al campo search
                                        if (!string.IsNullOrEmpty(urlServicioArchivos) && uriValida)
                                        {
                                            if (filaDocumento.ElementoVinculadoID.HasValue)
                                            {
                                                CallTokenService callTokenService = new CallTokenService(mConfigService);
                                                TokenBearer token = callTokenService.CallTokenApi();
                                                string result = CallWebMethods.CallGetApiToken(urlServicioArchivos, $"ObtenerXmlOntologia?pOntologiaID={filaDocumento.ElementoVinculadoID.Value}", token);
                                                byte[] byteArray = JsonConvert.DeserializeObject<byte[]>(result);

                                                if (byteArray != null)
                                                {
                                                    UtilidadesFormulariosSemanticos.ObtenerMetaEtiquetasXMLOntologia(byteArray, dicOntologiaMetas, filaDocumento.ElementoVinculadoID.Value);
                                                }
                                            }
                                            string subtipo = "";
                                            foreach (string triple in triples.Split(new[] { " ." }, StringSplitOptions.RemoveEmptyEntries))
                                            {
                                                if (triple.Contains("<http://gnoss/type>"))
                                                {
                                                    subtipo = triple.Substring(triple.LastIndexOf(">")).Replace(">", "").Replace("\"", "").Trim();
                                                    break;
                                                }
                                            }
                                            if (filaDocumento.ElementoVinculadoID.HasValue && (dicOntologiaMetas != null && dicOntologiaMetas.ContainsKey(filaDocumento.ElementoVinculadoID.Value)))
                                            {
                                                foreach (MetaKeyword metaTags in dicOntologiaMetas[filaDocumento.ElementoVinculadoID.Value])
                                                {
                                                    if (string.IsNullOrEmpty(metaTags.EntidadID) || metaTags.EntidadID.Equals(subtipo))
                                                    {
                                                        valorSearch += $" {metaTags.Content}";
                                                    }
                                                }
                                            }
                                        }

                                    }
                                    else if (!string.IsNullOrEmpty(rdfType))
                                    {
                                        if (!tripletasYaAgregadas)
                                        {
                                            mTripletas.Append(FacetadoAD.GenerarTripleta($"<http://gnoss/{idRecursoMay}>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", $"\"{rdfType}\""));
                                        }
                                    }
                                    else
                                    {
                                        if (!tripletasYaAgregadas)
                                        {
                                            mTripletas.Append(FacetadoAD.GenerarTripleta($"<http://gnoss/{idRecursoMay}>", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "\"Recurso\""));
                                        }
                                    }

                                    AnyadirTripleFoafFirstName(mTripletas.ToString(), ElementoID.Value, tripleFoafFirstName);

                                    //Semántico
                                    LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);
                                    //información para comunidad
                                    listaResultadosInformacionComunRecurso.AddRange(actualizacionFacetadoCN.ObtieneInformacionExtraRecurso(ElementoID.Value, proyID));
                                    foreach (QueryTriples resultado in listaResultadosInformacionComunRecurso)
                                    {
                                        string objeto = resultado.Objeto;
                                        if (resultado.Predicado.Contains("hastipodoc") && objeto.Contains("19"))
                                        { objeto = objeto.Replace("19", "2"); }
                                        if (resultado.Predicado.Contains("hastipodoc") && objeto.Contains("20"))
                                        { objeto = objeto.Replace("20", "21"); }
                                        if (resultado.Predicado.Contains("hastipodoc") && objeto.Contains("24"))
                                        { objeto = objeto.Replace("24", "2"); }
                                        if (resultado.Predicado.Contains("hastipodoc") && objeto.Contains("25"))
                                        { objeto = objeto.Replace("25", "21"); }
                                        if (!esborrador)
                                        {
                                            if (!tripletasYaAgregadas && resultado.Predicado != "<http://gnoss/hasautor>" && (resultado.Predicado != "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" || !string.IsNullOrEmpty(typeSem)))
                                            {
                                                mTripletas.Append(FacetadoAD.GenerarTripleta(resultado.Sujeto, resultado.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));
                                            }
                                            if (resultado.Predicado != "<http://gnoss/hasautor>" && (resultado.Predicado != "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>" || !string.IsNullOrEmpty(typeSem)))
                                            {
                                                mTripletasGnoss.Append(FacetadoAD.GenerarTripleta(resultado.Sujeto, resultado.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));
                                            }
                                        }
                                        mTripletasContribuciones.Append(FacetadoAD.GenerarTripleta(resultado.Sujeto, resultado.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));
                                    }
                                    LimpiarConfiguracionExceptoTablasSenialadas(facetaDS, listaTablasMantenerConfiguracion);
                                    #region borramos cache recursos (y RSS)
                                    if ((short)pFila["Prioridad"] < 11 || (short)pFila["Prioridad"] > 20)
                                    {
                                        if (filaProyecto.NumeroRecursos > 3000)
                                        {
                                            BaseComunidadCN baseComunidadCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                                            try
                                            {
                                                baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Recursos, null);
                                            }
                                            catch (Exception ex)
                                            {
                                                loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                                                baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Recursos);
                                            }

                                            if (typeSem != "")
                                            {
                                                try
                                                {
                                                    baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Recursos, $"rdf:type={typeSem}");
                                                }
                                                catch (Exception ex)
                                                {
                                                    loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                                                    baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Recursos, $"rdf:type={typeSem}");
                                                }

                                            }
                                            baseComunidadCN.Dispose();
                                        }
                                        else
                                        {
                                            FacetadoCL facetadoCL = new FacetadoCL(mFicheroConfiguracionBD, mFicheroConfiguracionBD, mUrlIntragnoss, entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                                            facetadoCL.Dominio = mDominio;
                                            facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(proyID, FacetadoAD.TipoBusquedaToString(TipoBusqueda.Recursos));
                                            facetadoCL.BorrarRSSDeComunidad(proyID);

                                            /*if (typeSem != "")
                                            {
                                                facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(proyID, "rdf:type=" + typeSem);
                                            }*/

                                            facetadoCL.Dispose();

                                            if (TieneComponenteConCaducidadTipoRecurso)
                                            {
                                                BaseComunidadCN baseComunidadCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                                                try
                                                {
                                                    baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.RefrescarComponentesRecursos, TipoBusqueda.Recursos, null);
                                                }
                                                catch (Exception ex)
                                                {
                                                    loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                                                    baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.RefrescarComponentesRecursos, TipoBusqueda.Recursos);
                                                }
                                                baseComunidadCN.Dispose();
                                            }
                                        }

                                        ProyectoCN proyCN = new ProyectoCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

                                        int refrescoNumeroResultados = proyCN.ObtenerNumRecursosProyecto(proyID);
                                        proyCN.Dispose();

                                        ProyectoCL proyCL = new ProyectoCL(mFicheroConfiguracionBD, entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                                        proyCL.Dominio = mDominio;
                                        proyCL.AgregarContadorComunidad(proyID, TipoBusqueda.Recursos, refrescoNumeroResultados);
                                    }
                                    #endregion
                                }

                                DocumentacionCL docCLRec = new DocumentacionCL("recursos", entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
                                docCLRec.Dominio = mDominio;
                                docCLRec.InvalidarFichaRecursoMVC(filaDocumento.DocumentoID, filaProyecto.ProyectoID);
                                docCLRec.Dispose();

                                DocumentacionCL docCL = new DocumentacionCL(entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
                                docCLRec.Dominio = mDominio;
                                docCL.InvalidarPerfilesConRecursosPrivados(filaProyecto.ProyectoID);
                                docCL.Dispose();
                            }
                        }
                    }
                }
                #endregion

                agregarTagsAModeloBase = false;
            }
            return ElementoID;
        }

        /// <summary>
        /// Verifica si en los triples semánticos hay ya un foaf:firstName y si no lo hay, inserta como foaf:firstName el título del recurso. 
        /// </summary>
        /// <param name="pTriplesRecursoSemantico">Triples del recurso semántico</param>
        /// <param name="pIdRecurso">Identificador del recurso</param>
        /// <param name="pTripleFoafFirstName">TripleFoafFirstName con el título del recurso</param>
        protected void AnyadirTripleFoafFirstName(string pTriplesRecursoSemantico, Guid pIdRecurso, string pTripleFoafFirstName)
        {
            string tripleBusqueda = $"<http://gnoss/{pIdRecurso.ToString().ToUpper()}> <http://xmlns.com/foaf/0.1/firstName> ";

            if (!pTriplesRecursoSemantico.Contains(tripleBusqueda))
            {
                mTripletas.Append(pTripleFoafFirstName);
            }
        }

        protected Guid? ProcesarFilaDeColaDeTipoAgregadoPersonasYOrganizaciones(DataRow pFila, Dictionary<short, List<string>> listaTagsFiltros, ref Dictionary<string, string> listaIdsEliminar, ref DataWrapperFacetas tConfiguracion, ActualizacionFacetadoCN actualizacionFacetadoCN, Guid proyID, ref string valorSearch, ref List<string> tags, ref bool agregarTagsAModeloBase, Proyecto filaProyecto, EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, EntityContextBASE entityContextBASE, RedisCacheWrapper redisCacheWrapper, UtilidadesVirtuoso utilidadesVirtuoso, GnossCache gnossCache, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            Guid? ElementoID = null;
            if (!(pFila is BasePerOrgComunidadDS.ColaTagsCom_Per_Org_ViRow))
            {
                //persona o organizacion

                //if pFila.Tags empieza por seguidores SEGUIDORES|Perfil1|Perfil2
                Guid idPersona = new Guid(listaTagsFiltros[(short)TiposTags.IDTagPer][0].ToUpper());

                //Añado tripleta MJ
                string organizacionopersona = listaTagsFiltros[(short)TiposTags.OrganizacionOPersona][0];
                if (organizacionopersona.Contains("##PERS-ORG##p##PERS-ORG##"))
                {
                    //Nombre y apellidos de la persona
                    string titulo = "";

                    Guid? idEnMyGnoss = actualizacionFacetadoCN.ObtieneIDIdentidad(ProyectoAD.MetaProyecto, idPersona, true);
                    if (idEnMyGnoss.HasValue)
                    {
                        InsertarTripletas_ControlCheckPoint(ObtenerPrioridadFila(pFila), ProyectoAD.MetaProyecto.ToString(), FacetadoAD.GenerarTripleta("<http://gnoss/" + idEnMyGnoss.Value.ToString().ToUpper() + ">", "<http://rdfs.org/sioc/ns#has_space>", "<http://gnoss/" + proyID.ToString().ToUpper() + ">"), 1, entityContext, loggingService, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                    }

                    Guid? id = actualizacionFacetadoCN.ObtieneIDIdentidad(proyID, idPersona, false);
                    ElementoID = id;
                    string idRecursoMayuscula = id.Value.ToString().ToUpper();
                    string idRecursoMinuscula = id.Value.ToString();

                    //REGION CONTACTOS
                    if (idEnMyGnoss.HasValue)
                    {
                        string tripletaContactos = FacetadoAD.GenerarTripleta("<http://gnoss/" + idEnMyGnoss.Value.ToString().ToUpper() + ">", "<http://rdfs.org/sioc/ns#has_space>", "<http://gnoss/" + proyID.ToString().ToUpper() + ">");
                        Guid usuarioID = actualizacionFacetadoCN.ObtenerIdUsuarioDesdeIdentidad(idEnMyGnoss.Value);
                        tripletaContactos += FacetadoAD.GenerarTripleta($"<http://gnoss/{usuarioID.ToString().ToUpper()}>", "<http://gnoss/IdentidadID>", $"<http://gnoss/{idEnMyGnoss.Value.ToString().ToUpper()}>");
                        InsertarTripletas_ControlCheckPoint(ObtenerPrioridadFila(pFila), "contactos", tripletaContactos, 0, entityContext, loggingService, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                    }

                    //FiN REGION CONTACTOS

                    //Se da el caso de que si el usuario está eliminado, tanto el id como el idEnMyGnoss vienen nulos y no se estaban controlando.
                    if (id.HasValue)
                    {
                        FacetadoCN facetadoCN = new FacetadoCN(mFicheroConfiguracionBD, mUrlIntragnoss, proyID.ToString(), entityContext, loggingService, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                        facetadoCN.FacetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                        //TODO cambiar nombre o nivel de participacion de identidad
                        //facetadoCN.ModificarParticipaciondeCooperativoaPersonal(ref mTripletas, proyID.ToString(), id.Value.ToString().ToUpper());

                        mTripletas.Append(FacetadoAD.GenerarTripleta("<http://gnoss/" + id.Value.ToString().ToUpper() + ">", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "\"Persona\""));


                        //16/02/2017 se decide dejar de meterlas: hablar con Juan y Esteban
                        //ObtenerTagsTitulo(titulo, id.Value);

                        ObtenerTagsEtiquetasDescompuestos(actualizacionFacetadoCN, "Persona", id.Value, proyID, ref tags, true);

                        //tags descompuestos titulo
                        titulo = actualizacionFacetadoCN.ObtenerTituloPersona(id.Value);
                        AgregarTripletasDescompuestas(id.Value, filaProyecto.ProyectoID, "<http://gnoss/hasTagTituloDesc>", titulo, true, false, tags);

                        mTripletas.Append(FacetadoAD.GenerarTripleta("<http://gnoss/" + id.Value.ToString().ToUpper() + ">", "<http://gnoss/hasnombrecompleto>", "\"" + UtilidadesVirtuoso.PasarObjetoALower(titulo) + "\""));
                        valorSearch += " " + titulo;



                        //Añado lo que no nos llegan los datos
                        //privacidad en comunidad
                        List<QueryTriples> listaTriplesPrivacidad = actualizacionFacetadoCN.ObtieneTripletasPrivacidadPersonas(id.Value, proyID, GestorParametroAplicacionDS.ParametroAplicacion);

                        foreach (QueryTriples query in listaTriplesPrivacidad)
                        {
                            if (query.Objeto != null)
                            {
                                string predicado = query.Predicado;
                                string predicadoLimpio = predicado.Replace("<", "").Replace(">", "").Replace("http://gnoss/", "").Trim();
                                string objeto = query.Objeto;

                                mTripletas.Append(FacetadoAD.GenerarTripleta(query.Sujeto.Replace(idRecursoMinuscula, idRecursoMayuscula), predicado, objeto));
                            }
                        }

                        //información extra persona en comunidad
                        List<QueryTriples> listaTriplesPersona = actualizacionFacetadoCN.ObtieneInformacionExtraPersona(id.Value, proyID);

                        foreach (QueryTriples query in listaTriplesPersona)
                        {
                            if (!string.IsNullOrEmpty(query.Objeto))
                            {
                                string predicado = query.Predicado;
                                string predicadoLimpio = predicado.Replace("<", "").Replace(">", "").Replace("http://gnoss/", "").Trim();
                                string objeto = (string)query.Objeto;

                                mTripletas.Append(FacetadoAD.GenerarTripleta((string)query.Sujeto.Replace(idRecursoMinuscula, idRecursoMayuscula), predicado, objeto));

                            }
                        }

                        //información extra persona para grafo de contactos
                        List<QueryTriples> listaInformacionExtraPersonaContactos = actualizacionFacetadoCN.ObtieneInformacionExtraPersonaContactos(id.Value, proyID);
                        string tripletaContactos2 = "";
                        foreach (QueryTriples query in listaInformacionExtraPersonaContactos)
                        {
                            if (!string.IsNullOrEmpty(query.Objeto))
                            {
                                string predicado = query.Predicado;
                                string predicadoLimpio = predicado.Replace("<", "").Replace(">", "").Replace("http://gnoss/", "").Trim();
                                string objeto = query.Objeto;

                                tripletaContactos2 += FacetadoAD.GenerarTripleta(query.Sujeto.Replace(idRecursoMinuscula, idRecursoMayuscula), predicado, objeto);
                            }
                        }

                        InsertarTripletas_ControlCheckPoint(ObtenerPrioridadFila(pFila), "contactos", tripletaContactos2, 0, entityContext, loggingService, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);

                        //Información de los seguidores:
                        List<QueryTriples> resultadoConsulta = actualizacionFacetadoCN.ObtenerIdentidadesSigueIdentidadDeProyecto(proyID, idPersona);

                        mTripletas.Append(PasarQueryTriplesATriples(resultadoConsulta));

                        //Region recursos comunidad
                        //Hay que modificar en virtuoso y remplazar el nombre del publicador de recursos con la misma identidad por el nuevo, porsiacaso.
                        //También el nombre de todos los editores de los recuross que coincidan con la identidad id

                        ActualizarPublicadorEditorRecursosComunidad_ControlCheckPoint(proyID, id.Value, titulo, (int)pFila["TablaBaseProyectoID"], loggingService, entityContext, virtuosoAD, entityContextBASE, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                    }
                }
                else if (organizacionopersona.Contains("##PERS-ORG##o##PERS-ORG##"))
                {
                    //Si es Organizacion
                    Guid? id = actualizacionFacetadoCN.ObtieneIDIdentidadOrg(proyID, idPersona, true);

                    IdentidadCN identCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                    bool estaExpulsada = identCN.EstaIdentidadExpulsada(id.Value);

                    if (!estaExpulsada)
                    {
                        //Compruebo si ha sido una baja voluntaria, en tal caso, la elimino del grafo de búsqueda
                        Guid? idIdentidadBaja = actualizacionFacetadoCN.ObtieneIDIdentidadOrg(proyID, idPersona);

                        if (!idIdentidadBaja.HasValue)
                        {
                            // La organización se ha dado de baja de la comunidad, no hay que añadirlo, sino eliminarlo
                            pFila["Tipo"] = 1;
                            ProcesarFilaDeCola(pFila, entityContext, loggingService, virtuosoAD, entityContextBASE, redisCacheWrapper, utilidadesVirtuoso, gnossCache, servicesUtilVirtuosoAndReplication);

                            return null;
                        }
                    }

                    Guid? idEnMyGnoss = actualizacionFacetadoCN.ObtieneIDIdentidadOrg(ProyectoAD.MetaProyecto, idPersona, true);

                    if (idEnMyGnoss.HasValue)
                    {
                        InsertarTripletas_ControlCheckPoint(ObtenerPrioridadFila(pFila), ProyectoAD.MetaProyecto.ToString(), FacetadoAD.GenerarTripleta("<http://gnoss/" + idEnMyGnoss.Value.ToString().ToUpper() + ">", "<http://rdfs.org/sioc/ns#has_space>", "<http://gnoss/" + proyID.ToString().ToUpper() + ">"), 1, entityContext, loggingService, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                    }

                    //REGION CONTACTOS
                    if (idEnMyGnoss.HasValue)
                    {
                        string tripletaContactos = FacetadoAD.GenerarTripleta("<http://gnoss/" + idEnMyGnoss.Value.ToString().ToUpper() + ">", "<http://rdfs.org/sioc/ns#has_space>", "<http://gnoss/" + proyID.ToString().ToUpper() + ">");

                        ProyectoAD proyectoAD = new ProyectoAD(mFicheroConfiguracionBD, loggingService, entityContext, mConfigService, servicesUtilVirtuosoAndReplication);
                        DataWrapperProyecto dataWrapperProyecto = new DataWrapperProyecto();

                        dataWrapperProyecto = proyectoAD.ObtenerProyectoPorID(proyID);
                        if (dataWrapperProyecto.ListaProyecto.FirstOrDefault().TipoAcceso.Equals((short)TipoAcceso.Privado))
                        {
                            tripletaContactos += FacetadoAD.GenerarTripleta("<http://gnoss/" + proyID.ToString().ToUpper() + ">", "<http://gnoss/hasprivacidadMyGnoss> ", "\"privado\"");
                        }

                        if (dataWrapperProyecto.ListaProyecto.FirstOrDefault().TipoAcceso.Equals((short)TipoAcceso.Publico))
                        {
                            tripletaContactos += FacetadoAD.GenerarTripleta("<http://gnoss/" + proyID.ToString().ToUpper() + ">", "<http://gnoss/hasprivacidadMyGnoss> ", "\"publico\"");
                        }

                        if (dataWrapperProyecto.ListaProyecto.FirstOrDefault().TipoAcceso.Equals((short)TipoAcceso.Reservado))
                        {
                            tripletaContactos += FacetadoAD.GenerarTripleta("<http://gnoss/" + proyID.ToString().ToUpper() + ">", "<http://gnoss/hasprivacidadMyGnoss> ", "\"reservado\"");
                        }

                        if (dataWrapperProyecto.ListaProyecto.FirstOrDefault().TipoAcceso.Equals((short)TipoAcceso.Restringido))
                        {
                            tripletaContactos += FacetadoAD.GenerarTripleta("<http://gnoss/" + proyID.ToString().ToUpper() + ">", "<http://gnoss/hasprivacidadMyGnoss> ", "\"restringido\"");
                        }

                        DataWrapperFacetas configuracionFacetadoDW = new DataWrapperFacetas();

                        Guid usuarioID = actualizacionFacetadoCN.ObtenerIdUsuarioDesdeIdentidad(idEnMyGnoss.Value);

                        tripletaContactos += FacetadoAD.GenerarTripleta($"<http://gnoss/{usuarioID.ToString()}>", "<http://gnoss/IdentidadID>", $"<http://gnoss/{idEnMyGnoss.Value.ToString().ToUpper()}>");

                        InsertarTripletas_ControlCheckPoint(ObtenerPrioridadFila(pFila), "contactos", tripletaContactos, 0, entityContext, loggingService, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                        //FiN REGION CONTACTOS
                    }

                    ElementoID = id;

                    string titulo = actualizacionFacetadoCN.ObtenerTituloOrganizacion(id.Value);
                    mTripletas.Append(FacetadoAD.GenerarTripleta("<http://gnoss/" + id.Value.ToString().ToUpper() + ">", "<http://gnoss/hasnombrecompleto>", "\"" + UtilidadesVirtuoso.PasarObjetoALower(titulo) + "\""));
                    valorSearch += " " + titulo;

                    //16/02/2017 se decide dejar de meterlas: hablar con Juan y Esteban
                    //ObtenerTagsTitulo(titulo, id.Value);

                    ObtenerTagsEtiquetasDescompuestos(actualizacionFacetadoCN, "Organizacion", id.Value, proyID, ref tags, true);

                    AgregarTripletasDescompuestas(id.Value, proyID, "<http://gnoss/hasTagTituloDesc>", titulo, true, false, tags);

                    //Añado lo que no nos llegan los datos
                    //información privacidad en comunidades
                    List<QueryTriples> listaTriplesPrivacidad = actualizacionFacetadoCN.ObtieneTripletasPrivacidadOrganizaciones(id.Value, proyID);

                    foreach (QueryTriples query in listaTriplesPrivacidad)
                    {
                        if (!string.IsNullOrEmpty(query.Objeto))
                        {
                            string objeto = query.Objeto;
                            mTripletas.Append(FacetadoAD.GenerarTripleta(query.Sujeto, query.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));
                        }
                    }

                    //obtiene información extra organización en comunidad
                    List<QueryTriples> listaInformacionExtraOrganizacion = actualizacionFacetadoCN.ObtieneInformacionExtraOrganizacion(id.Value, proyID);

                    foreach (QueryTriples query in listaInformacionExtraOrganizacion)
                    {
                        if (!string.IsNullOrEmpty(query.Objeto))
                        {
                            string objeto = query.Objeto;
                            mTripletas.Append(FacetadoAD.GenerarTripleta(query.Sujeto, query.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));
                        }
                    }
                }

                else if (organizacionopersona.Contains("##PERS-ORG##g##PERS-ORG##"))
                {
                    Guid? id = new Guid(listaTagsFiltros[(short)TiposTags.IDTagPer][0].ToUpper());
                    ElementoID = id;

                    string titulo = actualizacionFacetadoCN.ObtenerTituloGrupo(idPersona);
                    mTripletas.Append(FacetadoAD.GenerarTripleta("<http://gnoss/" + id.Value.ToString().ToUpper() + ">", "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "\"Grupo\""));
                    mTripletas.Append(FacetadoAD.GenerarTripleta("<http://gnoss/" + idPersona.ToString().ToUpper() + ">", "<http://gnoss/hasnombrecompleto>", "\"" + UtilidadesVirtuoso.PasarObjetoALower(titulo) + "\""));

                    mTripletas.Append(FacetadoAD.GenerarTripleta("<http://gnoss/" + idPersona.ToString().ToUpper() + ">", "<http://gnoss/hasPopularidad>", "-1"));
                    mTripletas.Append(FacetadoAD.GenerarTripleta("<http://gnoss/" + idPersona.ToString().ToUpper() + ">", "<http://gnoss/hasnumerorecursos>", "0"));

                    if (proyID != ProyectoAD.MetaProyecto)
                    {
                        ObtenerTagsEtiquetasDescompuestos(actualizacionFacetadoCN, "Grupo", id.Value, proyID, ref tags, false);
                        AgregarTripletasDescompuestas(idPersona, proyID, "<http://gnoss/hasTagTituloDesc>", titulo, false, false, tags);
                        valorSearch += " " + titulo;

                        //Añado lo que no nos llegan los datos
                        //información privacidad en comunidades
                        List<QueryTriples> listaTripletaPrivacidadGrupos = actualizacionFacetadoCN.ObtieneTripletasPrivacidadGrupos(id.Value);

                        foreach (QueryTriples query in listaTripletaPrivacidadGrupos)
                        {
                            if (!string.IsNullOrEmpty(query.Objeto))
                            {
                                string objeto = query.Objeto;
                                mTripletas.Append(FacetadoAD.GenerarTripleta(query.Sujeto, query.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));
                            }
                        }
                    }

                    // Region recursos comunidad
                    //Hay que modificar en virtuoso y remplazar el nombre del publicador de recursos con la misma identidad por el nuevo, porsiacaso.
                    //También el nombre de todos los editores de los recuross que coincidan con la identidad id

                    if (listaTagsFiltros.ContainsKey((short)TiposTags.PersonaNombreCompleto) && listaTagsFiltros[(short)TiposTags.PersonaNombreCompleto].Count > 0)
                    {
                        //en la edición de los grupos la fila deberá tener el filtro con el nombre viejo del grupo para filtrar la búsqueda
                        string nombreViejoGrupo = listaTagsFiltros[(short)TiposTags.PersonaNombreCompleto][0];
                        if (!string.IsNullOrEmpty(nombreViejoGrupo))
                        {
                            ActualizarGrupoLectorEditorRecursosComunidad_ControlCheckPoint(proyID, nombreViejoGrupo, titulo, (int)pFila["TablaBaseProyectoID"], loggingService, entityContext, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                        }
                    }
                }


                if ((short)pFila["Prioridad"] < 11 || (short)pFila["Prioridad"] > 20)
                {
                    if ((filaProyecto.NumeroMiembros + filaProyecto.NumeroOrgRegistradas) > 3000)
                    {
                        BaseComunidadCN baseComunidadCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                        try
                        {
                            baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.PersonasYOrganizaciones, null);
                        }
                        catch (Exception ex)
                        {
                            loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                            baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.PersonasYOrganizaciones);
                        }
                        baseComunidadCN.Dispose();
                    }
                    else
                    {
                        FacetadoCL facetadoCL = new FacetadoCL(mFicheroConfiguracionBD, mFicheroConfiguracionBD, mUrlIntragnoss, entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                        facetadoCL.Dominio = mDominio;
                        facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(proyID, FacetadoAD.TipoBusquedaToString(TipoBusqueda.PersonasYOrganizaciones));
                        facetadoCL.Dispose();
                    }
                }
            }
            agregarTagsAModeloBase = false;
            return ElementoID;
        }

        protected Guid? ProcesarFilaDeColaDeTipoAgregadoProyectos(DataRow pFila, Dictionary<short, List<string>> listaTagsFiltros, ref DataWrapperFacetas tConfiguracion, ActualizacionFacetadoCN actualizacionFacetadoCN, Guid proyID, ref string valorSearch, ref List<string> tags, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetaDS facetaDS = new FacetaDS();
            Guid idProyecto = new Guid(listaTagsFiltros[(short)TiposTags.IDTagProy][0].ToUpper());

            ObtenerTagsEtiquetasDescompuestos(actualizacionFacetadoCN, "Proyecto", idProyecto, proyID, ref tags, false);

            string titulo = actualizacionFacetadoCN.ObtenerTituloProyecto(idProyecto);
            AgregarTripletasDescompuestas(idProyecto, proyID, "<http://gnoss/hasTagTituloDesc>", titulo, false, false, tags);

            valorSearch += " " + titulo;

            mTripletas.Append(ObtenerTripletasCategoriasProyecto(idProyecto, ref valorSearch, entityContext, loggingService, redisCacheWrapper, servicesUtilVirtuosoAndReplication));

            //obtenemos información extra de la comunidad
            List<QueryTriples> listaInformacionExtraComunidad = actualizacionFacetadoCN.ObtieneInformacionExtraCom(idProyecto);

            foreach (QueryTriples query in listaInformacionExtraComunidad)
            {
                string objeto = query.Objeto;
                mTripletas.Append(FacetadoAD.GenerarTripleta(query.Sujeto, query.Predicado, UtilidadesVirtuoso.PasarObjetoALower(objeto)));
            }
            facetaDS.Clear();

            return idProyecto;
        }

        protected Guid? ProcesarFilaDeColaDeTipoAgregadoPaginaCMS(Dictionary<short, List<string>> pListaTagsFiltros, ref DataWrapperFacetas pTConfiguracion, ActualizacionFacetadoCN pActualizacionFacetadoCN, Guid pProyID, ref string pValorSearch, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            Guid pestanyaID = new Guid(pListaTagsFiltros[(short)TiposTags.IDPestanyaCMSProyecto][0].ToUpper());

            // Obtener los datos de la pestanya
            ProyectoCN proyCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            DataWrapperProyecto dataWrapperProyecto = new DataWrapperProyecto();
            proyCN.ObtenerPestanyasProyecto(pProyID, dataWrapperProyecto);
            proyCN.Dispose();

            List<ProyectoPestanyaMenu> pestanyas = dataWrapperProyecto.ListaProyectoPestanyaMenu.Where(proy => proy.PestanyaID.Equals(pestanyaID)).ToList();

            if (pestanyas.Count > 0)
            {
                ProyectoPestanyaMenu filaPestanyaMenu = pestanyas.First();

                string sujetoPestanya = "<http://gnoss/" + pestanyaID.ToString().ToUpper() + ">";

                // RDF:TYPE
                mTripletas.Append(FacetadoAD.GenerarTripleta(sujetoPestanya, "<http://www.w3.org/1999/02/22-rdf-syntax-ns#type>", "\"" + FacetadoAD.PAGINA_CMS + "\""));

                // FOAF:FIRSTNAME
                mTripletas.Append(FacetadoAD.GenerarTripleta(sujetoPestanya, "<http://xmlns.com/foaf/0.1/firstName>", "\"" + UtilidadesVirtuoso.PasarObjetoALower(filaPestanyaMenu.Nombre) + "\""));

                // GNOSS:HASTITULODESC
                mTripletas.Append(UtilidadesVirtuoso.AgregarTripletaDesnormalizadaTitulo(pestanyaID, filaPestanyaMenu.Nombre));

                // PRIVACIDAD
                AgregarTriplesPestanyaPrivacidad(pProyID, sujetoPestanya, (TipoPrivacidadPagina)filaPestanyaMenu.Privacidad, dataWrapperProyecto.ListaProyectoPestanyaMenuRolIdentidad, dataWrapperProyecto.ListaProyectoPestanyaMenuRolGrupoIdentidades, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

                // IDIOMA
                AgregarTriplesPestanyaIdioma(pProyID, sujetoPestanya, filaPestanyaMenu.IdiomasDisponibles, entityContext, loggingService, redisCacheWrapper, virtuosoAD, servicesUtilVirtuosoAndReplication);

                // PAGINAS PADRE PESTANYA ACTUAL

                // Solamente el primer padre

                if (filaPestanyaMenu.PestanyaPadreID.HasValue)
                {
                    mTripletas.Append(FacetadoAD.GenerarTripleta(sujetoPestanya, "<http://gnoss/hasPestanyaPadreID> ", "<http://gnoss/" + filaPestanyaMenu.PestanyaPadreID.ToString().ToUpper() + ">"));
                }


                AgregarTriplesPestanyaPadre(sujetoPestanya, dataWrapperProyecto, filaPestanyaMenu);

                // Obtener los datos de sus componentes CMS de tipo HTML Libre para calcular el SEARCH
                List<ProyectoPestanyaCMS> filaPestanyaCMS = dataWrapperProyecto.ListaProyectoPestanyaCMS.Where(proy => proy.PestanyaID.Equals(pestanyaID)).ToList();

                pValorSearch += filaPestanyaMenu.Ruta + " " + filaPestanyaMenu.Titulo + " " + filaPestanyaMenu.Nombre + " ";

                AgregarTriplePaginaCMSSearch(sujetoPestanya, filaPestanyaMenu.ProyectoID, filaPestanyaCMS, ref pValorSearch, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
                return pestanyaID;
            }
            else
            {
                return null;
            }
        }

        protected void AgregarTriplePaginaCMSSearch(string pSujetoPestanya, Guid pProyectoID, List<ProyectoPestanyaCMS> pFilasPestanyaCMS, ref string pValorSearch, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            CMSCN cmsCN = new CMSCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            foreach (ProyectoPestanyaCMS filaPestanyaCMS in pFilasPestanyaCMS)
            {
                // Obtener los componentes de la página del CMS
                DataWrapperCMS cmsDS = cmsCN.ObtenerCMSDeUbicacionDeProyecto(filaPestanyaCMS.Ubicacion, pProyectoID, 0, false);

                // Recorremos los componentes de tipo HTML Libre
                foreach (Es.Riam.Gnoss.AD.EntityModel.Models.CMS.CMSComponente cmsComponente in cmsDS.ListaCMSComponente.Where(item => item.TipoComponente.Equals((short)TipoComponenteCMS.HTML)))
                {
                    Es.Riam.Gnoss.AD.EntityModel.Models.CMS.CMSPropiedadComponente filaPropiedadComponente = cmsDS.ListaCMSPropiedadComponente.Where(item => item.ComponenteID.Equals(cmsComponente.ComponenteID) && item.TipoPropiedadComponente.Equals(0)).FirstOrDefault();
                    pValorSearch += filaPropiedadComponente.ValorPropiedad;
                }
            }
            cmsCN.Dispose();
        }

        protected void AgregarTriplesPestanyaPadre(string pSujetoPestanya, DataWrapperProyecto pDataWrapperProyecto, ProyectoPestanyaMenu pFilaPestanyaMenu)
        {
            mTripletas.Append(FacetadoAD.GenerarTripleta(pSujetoPestanya, "<http://gnoss/hasNombreCortoJerarquia> ", "\"" + pFilaPestanyaMenu.NombreCortoPestanya + "\""));

            if (pFilaPestanyaMenu.PestanyaPadreID.HasValue)
            {
                // Agregar triple pestanya actual
                ProyectoPestanyaMenu filaPestanyaMenuPadre = pDataWrapperProyecto.ListaProyectoPestanyaMenu.FirstOrDefault(proy => proy.PestanyaID.Equals(pFilaPestanyaMenu.PestanyaPadreID));
                AgregarTriplesPestanyaPadre(pSujetoPestanya, pDataWrapperProyecto, filaPestanyaMenuPadre);
            }
        }

        protected void AgregarTriplesPestanyaPrivacidad(Guid pProyectoID, string pSujetoPestanya, TipoPrivacidadPagina pPrivacidad, List<ProyectoPestanyaMenuRolIdentidad> pProyectoPestanyaMenuRolIdentidadDS, List<ProyectoPestanyaMenuRolGrupoIdentidades> pProyectoPestanyaMenuRolGrupoIdentidadesDS, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            string predicadoPrivacidadPestanya = "<http://gnoss/hasprivacidadCom> ";
            if (pPrivacidad == TipoPrivacidadPagina.Lectores)
            {
                mTripletas.Append(FacetadoAD.GenerarTripleta(pSujetoPestanya, predicadoPrivacidadPestanya, "\"privado\""));
            }
            else
            {
                ProyectoCN proyectoCN = new ProyectoCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                DataWrapperProyecto dataWrapperProyecto = new DataWrapperProyecto();
                dataWrapperProyecto = proyectoCN.ObtenerProyectoPorID(pProyectoID);
                proyectoCN.Dispose();

                bool proyectoPrivado = dataWrapperProyecto.ListaProyecto.FirstOrDefault().TipoAcceso.Equals((short)TipoAcceso.Privado) || dataWrapperProyecto.ListaProyecto.FirstOrDefault().TipoAcceso.Equals((short)TipoAcceso.Reservado);

                bool privacidadNormalComPublica_RecursoPublico = pPrivacidad == TipoPrivacidadPagina.Normal && !proyectoPrivado;
                bool privacidadEspecialComPrivada_RecursoPublico = pPrivacidad == TipoPrivacidadPagina.Especial && proyectoPrivado;

                bool privacidadNormalComPrivada_RecursoPublicoUsuariosRegistrados = pPrivacidad == TipoPrivacidadPagina.Normal && proyectoPrivado;
                bool privacidadEspecialComPublica_RecursoPublicoUsuariosRegistrados = pPrivacidad == TipoPrivacidadPagina.Especial && !proyectoPrivado;

                if (privacidadNormalComPublica_RecursoPublico || privacidadEspecialComPrivada_RecursoPublico)
                {
                    mTripletas.Append(FacetadoAD.GenerarTripleta(pSujetoPestanya, predicadoPrivacidadPestanya, "\"publico\""));
                }
                else if (privacidadNormalComPrivada_RecursoPublicoUsuariosRegistrados || privacidadEspecialComPublica_RecursoPublicoUsuariosRegistrados)
                {
                    mTripletas.Append(FacetadoAD.GenerarTripleta(pSujetoPestanya, predicadoPrivacidadPestanya, "\"publicoReg\""));
                }
            }

            AgregarTriplesPestanyaPrivacidadIdentidades(pProyectoID, pProyectoPestanyaMenuRolIdentidadDS, pSujetoPestanya, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

            AgregarTriplesPestanyaPrivacidadGrupoIdentidades(pProyectoPestanyaMenuRolGrupoIdentidadesDS, pSujetoPestanya, entityContext, loggingService, servicesUtilVirtuosoAndReplication);
        }

        protected void AgregarTriplesPestanyaPrivacidadIdentidades(Guid pProyectoID, List<ProyectoPestanyaMenuRolIdentidad> pProyectoPestanyaMenuRolIdentidadDS, string pSujetoPestanya, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            if (pProyectoPestanyaMenuRolIdentidadDS.Count > 0)
            {
                // Recorremos los DS de Roles, si hay alguna identidad o grupo, insertar en Virtuoso una referencia
                IdentidadCN identCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                foreach (ProyectoPestanyaMenuRolIdentidad filaPestanyaMenuRolIdentidad in pProyectoPestanyaMenuRolIdentidadDS)
                {
                    string predicadoPrivacidadIdentidad = "<http://gnoss/hasparticipanteIdentidadID> ";

                    // Obtener la IdentidadID del perfil 
                    Guid? identidadIDProyecto = identCN.ObtenerIdentidadIDDePerfilEnProyecto(pProyectoID, filaPestanyaMenuRolIdentidad.PerfilID);

                    string objetoIdentidadPermisoPestanya = "<http://gnoss/" + identidadIDProyecto.ToString().ToUpper() + "> .";
                    mTripletas.Append(FacetadoAD.GenerarTripleta(pSujetoPestanya, predicadoPrivacidadIdentidad, objetoIdentidadPermisoPestanya));
                }
                identCN.Dispose();
            }
        }

        protected void AgregarTriplesPestanyaPrivacidadGrupoIdentidades(List<ProyectoPestanyaMenuRolGrupoIdentidades> pProyectoPestanyaMenuRolGrupoIdentidadesDS, string pSujetoPestanya, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            if (pProyectoPestanyaMenuRolGrupoIdentidadesDS.Count > 0)
            {
                IdentidadCN identCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                foreach (ProyectoPestanyaMenuRolGrupoIdentidades filaPestanyaMenuRolGrupoIdentidades in pProyectoPestanyaMenuRolGrupoIdentidadesDS)
                {
                    string predicadoPrivacidadIdentidad = "<http://gnoss/hasparticipanteGrupoID> ";
                    string objetoGrupoID = "<http://gnoss/" + filaPestanyaMenuRolGrupoIdentidades.GrupoID.ToString().ToUpper() + ">";
                    mTripletas.Append(FacetadoAD.GenerarTripleta(pSujetoPestanya, predicadoPrivacidadIdentidad, objetoGrupoID + " ."));

                    // Si es un grupo de organización, hay que traer todas las identidades que participan en el grupo y meterlas en virtuoso
                    List<Guid> listaTemporalGrupo = new List<Guid>();
                    listaTemporalGrupo.Add(filaPestanyaMenuRolGrupoIdentidades.GrupoID);
                    DataWrapperIdentidad dataWrapperIdentidad = identCN.ObtenerGruposPorIDGrupo(listaTemporalGrupo);
                    if (dataWrapperIdentidad.ListaGrupoIdentidadesOrganizacion.Count > 0)
                    {
                        // Es un grupo de identidades de la organización, meter cada una de las identidades del grupo en virtuoso
                        foreach (Es.Riam.Gnoss.AD.EntityModel.Models.IdentidadDS.GrupoIdentidadesParticipacion identidadParticipacion in dataWrapperIdentidad.ListaGrupoIdentidadesParticipacion)
                        {
                            string predicadoParticipanteGrupoOrganizacionID = "<http://gnoss/hasparticipanteIdentidadID> ";
                            string objetoParticipanteGrupoOrganizacionID = "<http://gnoss/" + identidadParticipacion.IdentidadID.ToString().ToUpper() + "> .";
                            mTripletas.Append(FacetadoAD.GenerarTripleta(objetoGrupoID, predicadoParticipanteGrupoOrganizacionID, objetoParticipanteGrupoOrganizacionID));
                        }
                    }
                }
                identCN.Dispose();
            }
        }

        protected void AgregarTriplesPestanyaIdioma(Guid pProyectoID, string pSujetoPestanya, string pIdiomasPestanya, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            // Obtener el predicado de la tabla ParametroProyecto
            string predicadoIdiomaProyecto = ObtenerPredicadoIdiomas(pProyectoID, entityContext, loggingService, redisCacheWrapper, virtuosoAD, servicesUtilVirtuosoAndReplication);

            if (!string.IsNullOrEmpty(pIdiomasPestanya))
            {
                string[] delimiter = { "|||" };
                string[] idiomasPlataforma = pIdiomasPestanya.Split(delimiter, StringSplitOptions.RemoveEmptyEntries);
                foreach (string idioma in idiomasPlataforma)
                {
                    if (idioma.Contains("@"))
                    {
                        mTripletas.Append(FacetadoAD.GenerarTripleta(pSujetoPestanya, "<" + predicadoIdiomaProyecto + ">", "\"" + idioma.Split('@')[1] + "\""));
                    }
                }
            }
            else
            {
                // La pestanya está disponible en todos los idiomas de la plataforma. Consultar ParametroAplicación
                //List<ParametroAplicacion> filaParametroAplicacion = GestorParametroAplicacionDS.ParametroAplicacion.Select("Parametro = 'Idiomas'");
                List<ParametroAplicacion> filaParametroAplicacion = GestorParametroAplicacionDS.ParametroAplicacion.Where(parametroApp => parametroApp.Parametro.Equals("Idiomas")).ToList();
                if (filaParametroAplicacion.Count > 0)
                {
                    string[] delimiter = { "&&&" };
                    string[] idiomasPlataforma = filaParametroAplicacion[0].Valor.Split(delimiter, StringSplitOptions.RemoveEmptyEntries);
                    foreach (string idioma in idiomasPlataforma)
                    {
                        if (idioma.Contains("|"))
                        {
                            mTripletas.Append(FacetadoAD.GenerarTripleta(pSujetoPestanya, "<" + predicadoIdiomaProyecto + ">", "\"" + idioma.Split('|')[0] + "\""));
                        }
                    }
                }
            }
        }

        protected string ObtenerPredicadoIdiomas(Guid pProyectoID, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            string predicadoIdiomaProyecto = string.Empty;

            ProyectoCL proyectoCL = new ProyectoCL(mFicheroConfiguracionBD, entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
            Dictionary<string, string> parametroProyecto = proyectoCL.ObtenerParametrosProyecto(pProyectoID);
            proyectoCL.Dispose();

            if (parametroProyecto.ContainsKey(ParametroAD.PropiedadContenidoMultiIdioma))
            {
                predicadoIdiomaProyecto = parametroProyecto[ParametroAD.PropiedadContenidoMultiIdioma];
                if (predicadoIdiomaProyecto.Contains(":") && !predicadoIdiomaProyecto.Contains("http://"))
                {
                    string[] predicadoTroceado = predicadoIdiomaProyecto.Split(':');
                    if (predicadoTroceado.Length > 1 && FacetadoAD.ListaNamespacesBasicos.ContainsKey(predicadoTroceado[0]))
                    {
                        predicadoIdiomaProyecto = FacetadoAD.ListaNamespacesBasicos[predicadoTroceado[0]] + predicadoTroceado[1];
                    }
                    else
                    {
                        FacetaCL facetaCL = new FacetaCL(mFicheroConfiguracionBD, entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
                        Dictionary<string, List<string>> informacionOntologias = facetaCL.ObtenerPrefijosOntologiasDeProyecto(pProyectoID);
                        if (predicadoTroceado.Length > 1)
                        {
                            predicadoIdiomaProyecto = informacionOntologias.Where(diccionario => diccionario.Value.Contains(predicadoTroceado[0])).Select(dic => dic.Key).FirstOrDefault() + predicadoTroceado[1];
                            if (predicadoIdiomaProyecto.StartsWith("@"))
                            {
                                predicadoIdiomaProyecto = predicadoIdiomaProyecto.TrimStart('@');
                            }
                        }
                    }
                }
            }
            else
            {
                // Ponemos por defecto el predicado dce:language
                predicadoIdiomaProyecto = "http://purl.org/dc/elements/1.1/language";
            }

            return predicadoIdiomaProyecto;
        }

        #endregion

        #region Métodos auxiliares para ProcesarFilaDeColaDeTipoAgregado

        protected void LimpiarConfiguracionExceptoTablasSenialadas(FacetaDS tConfiguracion, List<string> pListaTablas)
        {
            foreach (DataTable tabla in tConfiguracion.Tables)
            {
                if (!pListaTablas.Contains(tabla.TableName))
                {
                    tabla.Clear();
                }
            }
        }

        protected void ObtenerTagsEtiquetasDescompuestos(ActualizacionFacetadoCN actualizacionFacetadoCN, string tipoItem, Guid idElemento, Guid idProyecto, ref List<string> tags, bool incluirTripletasGnoss)
        {
            //tags etiquetas descompuestos
            foreach (string tag in actualizacionFacetadoCN.ObtenerTags(idElemento, tipoItem, idProyecto))
            {
                //AgregarTripletasDescompuestas(idElemento, idProyecto, "<http://gnoss/hasTagDesc>", tag, false, false);

                string objeto = "\"" + tag.Replace("\"", "'").Trim() + "\" .";

                mTripletas.Append(FacetadoAD.GenerarTripleta("<http://gnoss/" + idElemento.ToString().ToUpper() + ">", "<http://rdfs.org/sioc/types#Tag>", objeto));
                /*if (incluirTripletasGnoss)
                {
                    mTripletasGnoss.Append(FacetadoAD.GenerarTripleta("<http://gnoss/" + idElemento.ToString().ToUpper() + ">", "<http://rdfs.org/sioc/types#Tag>", objeto));
                }*/

                // 20160323 Triodos Alberto: Elimino el codigo que agrega tags descompuestos sin acentos

                tags.Add(tag);
            }
        }

        #endregion

        #endregion

        #region ProcesarFilaDeColaDeTipoEliminado

        protected void ProcesarFilaDeColaDeTipoEliminado(ref DataRow pFila, List<string> listaTodosTags, List<string> listaTagsDirectos, List<string> listaTagsIndirectos, Dictionary<short, List<string>> listaTagsFiltros, EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, EntityContextBASE entityContextBASE, RedisCacheWrapper redisCacheWrapper, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            Guid? idEnMyGnoss = null;

            #region Borrar en virtuoso
            ProyectoCN proyectoCN = new ProyectoCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            Guid proyID = ProyectoAD.MetaProyecto;
            Proyecto filaProyecto = null;
            short estadoProy = 2;
            int numeroPreguntas = 0;
            int numeroDebates = 0;
            int numeroRecursos = 0;
            int numeroPersonasYOrg = 0;

            if ((int)pFila["TablaBaseProyectoID"] != 0)
            {
                filaProyecto = proyectoCN.ObtenerProyectoPorTablaBaseProyectoID((int)pFila["TablaBaseProyectoID"]).ListaProyecto.FirstOrDefault();

                proyID = filaProyecto.ProyectoID;
                estadoProy = filaProyecto.Estado;
                if (filaProyecto.NumeroDebates.HasValue)
                {
                    numeroDebates = filaProyecto.NumeroDebates.Value;
                }
                if (filaProyecto.NumeroPreguntas.HasValue)
                {
                    numeroPreguntas = filaProyecto.NumeroPreguntas.Value;
                }
                if (filaProyecto.NumeroRecursos.HasValue)
                {
                    numeroRecursos = filaProyecto.NumeroRecursos.Value;
                }
                if (filaProyecto.NumeroMiembros.HasValue && filaProyecto.NumeroOrgRegistradas.HasValue)
                {
                    numeroPersonasYOrg = filaProyecto.NumeroMiembros.Value + filaProyecto.NumeroOrgRegistradas.Value;
                }
                else if (filaProyecto.NumeroMiembros.HasValue)
                {
                    numeroPersonasYOrg = filaProyecto.NumeroMiembros.Value;
                }
                else if (filaProyecto.NumeroOrgRegistradas.HasValue)
                {
                    numeroPersonasYOrg = filaProyecto.NumeroOrgRegistradas.Value;
                }
            }

            FacetaDS tConfiguracion = new FacetaDS();

            Guid? id = null;
            List<Guid> listaIdentidades = new List<Guid>();

            FacetadoCN facetadoCN2 = new FacetadoCN(mUrlIntragnoss, proyID.ToString(), entityContext, loggingService, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
            facetadoCN2.FacetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;
            FacetadoDS facetadoCVDS = new FacetadoDS();

            bool borrarEnVirtuoso = false;
            bool borrarAuxiliar = false;

            bool esComentario = false;

            if (pFila.Table.DataSet is BaseRecursosComunidadDS)
            {
                if (!(pFila is BaseRecursosComunidadDS.ColaTagsMyGnossRow))
                {
                    id = new Guid(listaTagsFiltros[(short)TiposTags.IDTagDoc][0]);
                    borrarEnVirtuoso = true;

                    //Se ha eliminado un recurso
                    #region ColaSiteMaps

                    if (filaProyecto != null)
                    {
                        ParametroGeneralCN paramGralCN = new ParametroGeneralCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                        ParametroGeneral filaParamGral = paramGralCN.ObtenerFilaParametrosGeneralesDeProyecto(filaProyecto.ProyectoID);
                        paramGralCN.Dispose();

                        BaseComunidadCN baseComunidadCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                        DocumentacionCN docCN = new DocumentacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                        Guid documentoID = id.Value;
                        DataWrapperDocumentacion docDW = docCN.ObtenerDocumentoPorID(documentoID);

                        string comentarioorecurso = " ";
                        if (listaTagsFiltros[(short)TiposTags.ComentarioORecurso].Count > 0)
                        {
                            comentarioorecurso = listaTagsFiltros[(short)TiposTags.ComentarioORecurso][0];
                        }
                        if (comentarioorecurso.Contains("c"))
                        {
                            ActualizarNumComentariosVirtuoso(documentoID, entityContext, loggingService, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                            esComentario = true;
                        }


                        //si la comunidad tiene el sitemap generado
                        if (filaParamGral.TieneSitemapComunidad)
                        {
                            if (docDW.ListaDocumento.Count > 0)
                            {
                                DateTime fechaCreacionDoc = docDW.ListaDocumento.First().FechaCreacion.Value;
                                baseComunidadCN.InsertarFilaEnColaColaSitemaps(documentoID, TiposEventoSitemap.RecursoEliminado, 0, fechaCreacionDoc, 1, filaProyecto.NombreCorto);
                            }
                        }

                        if (docDW.ListaDocumentoWebVinBaseRecursos.Count > 0)
                        {
                            //se agrega la fila a la ColaActualizarContextos
                            //baseComunidadCN.InsertarFilaEnColaActualizaContextos(id.Value, 0, (short)pFila["Prioridad"], docDW.ListaDocumentoWebVinBaseRecursos.First().FechaPublicacion.Value);
                        }

                        string tipoDoc = listaTagsFiltros[(short)TiposTags.TipoDocumento][0];
                        if (tipoDoc.Contains("5"))
                        {
                            borrarAuxiliar = true;
                        }

                        if ((short)pFila["Prioridad"] < 11 || (short)pFila["Prioridad"] > 20)
                        {
                            FacetadoCL facetadoCL = new FacetadoCL(mFicheroConfiguracionBD, mUrlIntragnoss, entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                            facetadoCL.Dominio = mDominio;

                            bool componentesCMSActualizados = false;

                            string enlace = docCN.ObtenerEnlaceDocumentoVinculadoADocumento(documentoID);
                            string infoExtra = string.Empty;

                            if (!string.IsNullOrEmpty(enlace))
                            {
                                infoExtra = "rdf:type=" + enlace.Substring(0, enlace.IndexOf("."));
                            }

                            if (tipoDoc.Contains("15"))
                            {
                                if (filaProyecto.NumeroPreguntas > 3000)
                                {
                                    try
                                    {
                                        baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Preguntas, null);
                                    }
                                    catch (Exception ex)
                                    {
                                        loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                                        baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Preguntas);
                                    }
                                    componentesCMSActualizados = true;
                                }
                                else
                                {
                                    facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(proyID, FacetadoAD.TipoBusquedaToString(TipoBusqueda.Preguntas));
                                    facetadoCL.BorrarRSSDeComunidad(proyID);
                                }
                            }
                            else if (tipoDoc.Contains("16"))
                            {
                                if (filaProyecto.NumeroDebates > 3000)
                                {
                                    try
                                    {
                                        baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Debates, null);
                                    }
                                    catch (Exception ex)
                                    {
                                        loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                                        baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Debates);
                                    }

                                    componentesCMSActualizados = true;
                                }
                                else
                                {
                                    facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(proyID, FacetadoAD.TipoBusquedaToString(TipoBusqueda.Debates));
                                    facetadoCL.BorrarRSSDeComunidad(proyID);
                                }
                            }
                            else if (tipoDoc.Contains("16"))
                            {
                                if (filaProyecto.NumeroDebates > 3000)
                                {
                                    try
                                    {
                                        baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Debates, null);
                                    }
                                    catch (Exception ex)
                                    {
                                        loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                                        baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Debates);
                                    }

                                    componentesCMSActualizados = true;
                                }
                                else
                                {
                                    facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(proyID, FacetadoAD.TipoBusquedaToString(TipoBusqueda.Debates));
                                    facetadoCL.BorrarRSSDeComunidad(proyID);
                                }
                            }
                            else if (tipoDoc.Contains("18"))
                            {
                                facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(proyID, FacetadoAD.TipoBusquedaToString(TipoBusqueda.Encuestas));
                                facetadoCL.BorrarRSSDeComunidad(proyID);
                            }
                            else
                            {
                                if (filaProyecto.NumeroRecursos > 3000)
                                {
                                    try
                                    {
                                        baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Recursos, null);
                                    }
                                    catch (Exception ex)
                                    {
                                        loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                                        baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.Recursos);
                                    }
                                    componentesCMSActualizados = true;
                                }
                                else
                                {
                                    facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(proyID, FacetadoAD.TipoBusquedaToString(TipoBusqueda.Recursos));
                                    facetadoCL.BorrarRSSDeComunidad(proyID);
                                }
                            }

                            if (!componentesCMSActualizados)
                            {
                                try
                                {
                                    baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.RefrescarComponentesRecursos, TipoBusqueda.Recursos, null);
                                }
                                catch (Exception ex)
                                {
                                    loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                                    baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.RefrescarComponentesRecursos, TipoBusqueda.Recursos);
                                }
                            }

                            baseComunidadCN.Dispose();
                            docCN.Dispose();
                            facetadoCL.Dispose();
                        }
                    }

                    #endregion
                }
            }
            else if (pFila.Table.DataSet is BasePerOrgComunidadDS)
            {
                if (!(pFila is BasePerOrgComunidadDS.ColaTagsCom_Per_Org_ViRow))
                {
                    string organizacionopersona = listaTagsFiltros[(short)TiposTags.OrganizacionOPersona][0];
                    ActualizacionFacetadoCN actualizacionFacetadoCN = new ActualizacionFacetadoCN(mFicheroConfiguracionBD, mUrlIntragnoss, entityContext, loggingService, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    Guid personaID = new Guid(listaTagsFiltros[(short)TiposTags.IDTagPer][0]);

                    if (organizacionopersona.Contains("##PERS-ORG##p##PERS-ORG##"))
                    {
                        idEnMyGnoss = actualizacionFacetadoCN.ObtieneIDIdentidad(ProyectoAD.MetaProyecto, personaID, true);
                        id = actualizacionFacetadoCN.ObtieneIDIdentidad(proyID, personaID, true);
                        listaIdentidades = actualizacionFacetadoCN.ObtieneListaIDsIdentidad(proyID, personaID, true);

                        if (id.HasValue)
                        {
                            facetadoCN2.ObtenerIDDocCVDesdeVirtuoso(facetadoCVDS, proyID.ToString(), id.Value.ToString());
                            FacetadoCN facetadoCN = new FacetadoCN(mUrlIntragnoss, proyID.ToString(), entityContext, loggingService, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                            facetadoCN.FacetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;
                            //TODO cambiar nombre o nivel de participacion de identidad
                            //mTripletasCambioCorporativoPersonal= facetadoCN.ModificarParticipaciondeCooperativoaPersonal(proyID, id.Value);
                        }
                    }
                    else
                    {
                        idEnMyGnoss = actualizacionFacetadoCN.ObtieneIDIdentidadOrg(ProyectoAD.MetaProyecto, id.Value, true);
                        id = actualizacionFacetadoCN.ObtieneIDIdentidadOrg(proyID, id.Value, true);
                    }

                    if (idEnMyGnoss.HasValue)
                    {
                        #region CONTACTOS

                        BorrarTripleta_ControlCheckPoint("contactos", "<http://gnoss/" + idEnMyGnoss + ">", "<http://rdfs.org/sioc/ns#has_space>", "<http://gnoss/" + proyID.ToString().ToUpper() + ">", entityContext, loggingService, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);

                        Guid usuarioID = actualizacionFacetadoCN.ObtenerIdUsuarioDesdeIdentidad(idEnMyGnoss.Value);

                        BorrarTripleta_ControlCheckPoint("contactos", $"<http://gnoss/{usuarioID}>", "<http://gnoss/IdentidadID>", $"<http://gnoss/{idEnMyGnoss.ToString().ToUpper()}>", entityContext, loggingService, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                        #endregion
                    }

                    borrarEnVirtuoso = true;

                    #region borramos cache personas y org
                    if ((short)pFila["Prioridad"] < 11 || (short)pFila["Prioridad"] > 20)
                    {
                        if (numeroPersonasYOrg > 3000)
                        {
                            BaseComunidadCN baseComunidadCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                            try
                            {
                                baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.PersonasYOrganizaciones, null);
                            }
                            catch (Exception ex)
                            {
                                loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                                baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.PersonasYOrganizaciones);
                            }

                            baseComunidadCN.Dispose();
                        }
                        else
                        {
                            FacetadoCL facetadoCL = new FacetadoCL(mFicheroConfiguracionBD, mFicheroConfiguracionBD, mUrlIntragnoss, entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                            facetadoCL.Dominio = mDominio;
                            facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(proyID, FacetadoAD.TipoBusquedaToString(TipoBusqueda.PersonasYOrganizaciones));
                            facetadoCL.Dispose();
                        }
                    }
                    #endregion
                }
            }
            else if (pFila.Table.DataSet is BaseProyectosDS)
            {
                id = new Guid(listaTagsFiltros[(short)TiposTags.IDTagProy][0]);
                borrarEnVirtuoso = true;
            }
            else if (pFila.Table.DataSet is BasePaginaCMSDS)
            {
                id = new Guid(listaTagsFiltros[(short)TiposTags.IDPestanyaCMSProyecto][0]);
                borrarEnVirtuoso = true;
            }

            if (borrarEnVirtuoso)
            {
                DocumentacionCN docCN = new DocumentacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                if (id.HasValue)
                {
                    ParametroAplicacion filaParametro = GestorParametroAplicacionDS.ParametroAplicacion.Find(parametroApp => parametroApp.Parametro.Equals(TiposParametrosAplicacion.GenerarGrafoContribuciones));

                    bool generarGrafoContribuciones = (filaParametro == null || filaParametro.Valor.Equals("1"));

                    if (generarGrafoContribuciones)
                    {

                        Guid identidadcreador = docCN.ObtenerPublicadorAPartirIDsRecursoYProyecto(proyID, id.Value);

                        if (esComentario)
                        {
                            identidadcreador = docCN.ObtenerPublicadorAPartirIDsComentario(id.Value);
                        }

                        IdentidadCN idenCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);

                        List<Guid> resultado2 = idenCN.ObtenerPerfilyOrganizacionID(identidadcreador);

                        if (resultado2.Count > 1 && resultado2[1] != null)
                        {
                            Guid orgID = new Guid(resultado2[1].ToString());
                            facetadoCN2.BorrarRecurso(orgID.ToString(), id.Value, 0, "", false, borrarAuxiliar);
                        }
                        else if (resultado2.Count > 0)
                        {
                            Guid perfilID = new Guid(resultado2[0].ToString());
                            facetadoCN2.BorrarRecurso(perfilID.ToString(), id.Value, 0, "", false, borrarAuxiliar);
                        }
                    }

                    //ObtenerPerfiles en los que esta compartido y eliminado

                    List<Guid> perfilesConEsteRecursoEliminado = docCN.ObtenerPerfilesIDEstaCompartidoYEliminadoRecurso(id.Value);
                    foreach (Guid perfil in perfilesConEsteRecursoEliminado)
                    {
                        facetadoCN2.BorrarRecurso(perfil.ToString(), id.Value, 0, "", false, borrarAuxiliar);
                    }

                    if (listaIdentidades.Count > 0)
                    {
                        foreach (Guid identidadID in listaIdentidades)
                        {
                            facetadoCN2.BorrarRecurso(proyID.ToString(), identidadID, 0, "", false, borrarAuxiliar, !(pFila.Table.DataSet is BaseRecursosComunidadDS));
                        }
                    }
                    else
                    {
                        facetadoCN2.BorrarRecurso(proyID.ToString(), id.Value, 0, "", false, borrarAuxiliar, !(pFila.Table.DataSet is BaseRecursosComunidadDS));
                    }


                }
            }

            #endregion
        }

        #endregion

        #region ProcesarFilaDeColaDeTipoNivelesCertificacionModificados

        protected void ProcesarFilaDeColaDeTipoNivelesCertificacionModificados(ref DataRow pFila, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            Guid proyID = ProyectoAD.MetaProyecto;
            #region Obtenemos Proyecto
            ProyectoCN proyectoCN = new ProyectoCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            if ((int)pFila["TablaBaseProyectoID"] != 0)
            {
                Proyecto filaProy = proyectoCN.ObtenerProyectoPorTablaBaseProyectoID((int)pFila["TablaBaseProyectoID"]).ListaProyecto.FirstOrDefault();
                proyID = filaProy.ProyectoID;
            }
            proyectoCN.Dispose();
            #endregion

            if (proyID != ProyectoAD.MetaProyecto)
            {
                DocumentacionCN docCN = new DocumentacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                Dictionary<int, List<Guid>> listaNivelesRecursos = docCN.ObtenerNivelesCertificacionDeDocsEnProyecto(proyID);
                docCN.Dispose();

                FacetadoCN facetadoCN = new FacetadoCN(mFicheroConfiguracionBD, mUrlIntragnoss, proyID.ToString(), entityContext, loggingService, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoCN.ModificarCertificacionesRecursos(proyID, listaNivelesRecursos);
                facetadoCN.Dispose();

                //Invalidamos la cache de los niveles de certificación                           
                ProyectoCL proyCL = new ProyectoCL(mFicheroConfiguracionBD, entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                proyCL.Dominio = mDominio;
                proyCL.InvalidarNivelesCertificacionRecursosProyecto(proyID);
                proyCL.Dispose();
            }
        }

        #endregion

        #region ProcesarFilaDeColaDeCategoriasRecategorizadas

        protected void ProcesarFilaDeColaDeCategoriasRecategorizadas(ref DataRow pFila, Dictionary<short, List<string>> listaTagsFiltros, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, VirtuosoAD virtuosoAD, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            IdentidadCN idenCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            UsuarioCN usuarioCN = new UsuarioCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            OrganizacionCN organizacionCN = new OrganizacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            //Obtengo el identificador del proyecto
            ProyectoCN proyectoCN = new ProyectoCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            Guid proyID = ProyectoAD.MetaProyecto;
            short estadoProy = 2;
            int numeroPreguntas = 0;
            int numeroDebates = 0;
            int numeroRecursos = 0;
            int numeroPersonasYOrg = 0;

            if ((int)pFila["TablaBaseProyectoID"] != 0)
            {
                Proyecto filaProy = proyectoCN.ObtenerProyectoPorTablaBaseProyectoID((int)pFila["TablaBaseProyectoID"]).ListaProyecto.FirstOrDefault();
                estadoProy = filaProy.Estado;
                proyID = filaProy.ProyectoID;
                if (filaProy.NumeroDebates.HasValue)
                {
                    numeroDebates = filaProy.NumeroDebates.Value;
                }
                if (filaProy.NumeroPreguntas.HasValue)
                {
                    numeroPreguntas = filaProy.NumeroPreguntas.Value;
                }
                if (filaProy.NumeroRecursos.HasValue)
                {
                    numeroRecursos = filaProy.NumeroRecursos.Value;
                }
                if (filaProy.NumeroMiembros.HasValue && filaProy.NumeroOrgRegistradas.HasValue)
                {
                    numeroPersonasYOrg = filaProy.NumeroMiembros.Value + filaProy.NumeroOrgRegistradas.Value;
                }
                else if (filaProy.NumeroMiembros.HasValue)
                {
                    numeroPersonasYOrg = filaProy.NumeroMiembros.Value;
                }
                else if (filaProy.NumeroOrgRegistradas.HasValue)
                {
                    numeroPersonasYOrg = filaProy.NumeroOrgRegistradas.Value;
                }
            }
            string idProy = proyID.ToString();
            //Obtengo el identificador de la categoría
            List<string> listaCategorias = listaTagsFiltros[(short)TiposTags.CategoriaTesauro];
            string catEliminar = listaCategorias[0];
            string catNueva = listaCategorias[1];

            bool perfilpersonal = false;
            if (idProy.Equals(ProyectoAD.MetaProyecto.ToString()))
            {
                perfilpersonal = true;

                idProy = idenCN.ObtenerPerfilIDPorIDTesauro(new Guid(catEliminar)).ToString();
            }

            #region Cargamos el tesauro
            //Cargamos el tesauro de la comunidad (o perfil)
            TesauroCN tesCN = new TesauroCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            GestionTesauro gestorTesauro;
            if (!perfilpersonal)
            {
                gestorTesauro = new GestionTesauro(tesCN.ObtenerTesauroDeProyecto(proyID), loggingService, entityContext);
            }
            else
            {
                Guid? usID = usuarioCN.ObtenerUsuarioIDPorIDTesauro(new Guid(catEliminar));

                if (usID == null)
                {
                    Guid? orgID = organizacionCN.ObtenerOrganizacionIDPorIDTesauro(new Guid(catEliminar));
                    idProy = orgID.Value.ToString();

                    Guid tesID = tesCN.ObtenerIDTesauroDeOrganizacion(orgID.Value);
                    gestorTesauro = new GestionTesauro(tesCN.ObtenerTesauroCompletoPorID(tesID), loggingService, entityContext);
                }
                else
                {
                    idProy = usID.Value.ToString();

                    Guid tesID = tesCN.ObtenerIDTesauroDeUsuario(usID.Value);
                    gestorTesauro = new GestionTesauro(tesCN.ObtenerTesauroCompletoPorID(tesID), loggingService, entityContext);
                }
            }
            tesCN.Dispose();
            #endregion

            Guid tesauroID = gestorTesauro.TesauroDW.ListaTesauro.First().TesauroID;

            //Cargamos todos los recursos de la categoria destino
            DocumentacionCN docCN = new DocumentacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            #region Obtenemos recursos a modificar
            //Obtenemos todos los recursos del tesauro recursos de la categoría destino
            //List<Guid> listaDocsID = docCN.ObtenerListaDocsAgCatDeTesauroID(tesauroID);

            //Obtenemos los recursos de la categoría destino
            List<Guid> listaDocsID = docCN.ObtenerListaDocsAgCatDeTesauroID(new Guid(catNueva));

            ////Obtenemos los recursos vinculados a la categoría 'eliminada'
            //FacetadoDS facetadoCategoriaDS = facetadoAD.ObtieneElementosDeCategoria(idProy, catEliminar);
            FacetadoDS facetadoCategoriaDS = ObtieneElementosDeCategoria_ControlCheckPoint(idProy, catEliminar, loggingService, entityContext, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
            foreach (DataRow filaElemento in facetadoCategoriaDS.Tables["Resultados"].Rows)
            {
                Guid id = new Guid(((string)filaElemento[0]).Replace("http://gnoss/", ""));
                if (!listaDocsID.Contains(id))
                {
                    listaDocsID.Add(id);
                }
            }
            #endregion

            List<Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.DocumentoWebAgCatTesauroConVinculoTesauroID> listaDocumentoWebAgCatTesauroConVinculoTesauroID = docCN.ObtenerCategoriasTesauroYTesauroDeDocumentos(listaDocsID);
            docCN.Dispose();


            int numeroTripletas = 0;
            Dictionary<string, string> listaIds = new Dictionary<string, string>();

            //Cargamos el Documento con las categorías a las que están vinculados y sus padres
            Dictionary<Guid, List<Guid>> diccionarioDocumentoCategorias = new Dictionary<Guid, List<Guid>>();
            foreach (Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.DocumentoWebAgCatTesauroConVinculoTesauroID filaAgCat in listaDocumentoWebAgCatTesauroConVinculoTesauroID)
            {
                if (tesauroID == filaAgCat.TesauroID)
                {
                    List<Guid> categorias = new List<Guid>();

                    #region Cargamos la categoría y los padres
                    CategoriaTesauro categoria = gestorTesauro.ListaCategoriasTesauro[filaAgCat.CategoriaTesauroID];
                    categorias.Add(categoria.Clave);

                    IElementoGnoss padre = categoria.Padre;
                    while ((padre != null) && (padre is CategoriaTesauro))
                    {
                        Guid clave = ((CategoriaTesauro)padre).Clave;
                        if (!categorias.Contains(clave))
                        {
                            categorias.Add(clave);
                        }
                        padre = padre.Padre;
                    }
                    #endregion

                    if (!diccionarioDocumentoCategorias.ContainsKey(filaAgCat.DocumentoID))
                    {
                        diccionarioDocumentoCategorias.Add(filaAgCat.DocumentoID, categorias);
                    }
                    else
                    {
                        foreach (Guid categoriaID in categorias)
                        {
                            if (!diccionarioDocumentoCategorias[filaAgCat.DocumentoID].Contains(categoriaID))
                            {
                                diccionarioDocumentoCategorias[filaAgCat.DocumentoID].Add(categoriaID);
                            }
                        }
                    }
                }
            }

            foreach (Guid docID in diccionarioDocumentoCategorias.Keys)
            {
                foreach (Guid catID in diccionarioDocumentoCategorias[docID])
                {
                    string sujeto = "<http://gnoss/" + docID.ToString().ToUpper() + "> ";
                    string predicado = "<http://www.w3.org/2004/02/skos/core#ConceptID> ";
                    string objeto = "<http://gnoss/" + catID.ToString().ToUpper() + "> .";

                    mTripletas.Append(sujeto + predicado + objeto + " \n ");
                    numeroTripletas++;

                    string nombrecategoria = gestorTesauro.TesauroDW.ListaCategoriaTesauro.FirstOrDefault(item => item.CategoriaTesauroID.Equals(catID)).Nombre;

                    mTripletas.Append("<http://gnoss/" + catID.ToString().ToUpper() + ">" + "<http://gnoss/CategoryName>" + "\"" + nombrecategoria.ToLower() + "\" . \n ");
                    numeroTripletas++;

                    if (!listaIds.ContainsKey(docID.ToString().ToUpper()))
                    {
                        listaIds.Add(docID.ToString().ToUpper(), "");
                    }
                    if (numeroTripletas > 500)
                    {   //Si se trata de las categoria de usuario añadimos al grafo del perfil

                        InsertaTripletasConModify_ControlCheckPoint(ObtenerPrioridadFila(pFila), idProy, mTripletas.ToString(), listaIds, "", " ?p = skos:ConceptID ", false, loggingService, entityContext, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                        //facetadoAD.InsertaTripletasConModify(idProy, mTripletas, listaIds, "", " ?p = skos:ConceptID ");
                        mTripletas.Clear();
                        listaIds.Clear();
                        numeroTripletas = 0;
                    }
                }
            }
            gestorTesauro.Dispose();

            if (numeroTripletas > 0)
            {
                //Si se trata de las categoria de usuario añadimos al grafo del perfil
                InsertaTripletasConModify_ControlCheckPoint(ObtenerPrioridadFila(pFila), idProy, mTripletas.ToString(), listaIds, "", " ?p = skos:ConceptID ", false, loggingService, entityContext, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
            }

            #region borramos cache recursos (y RSS)
            if ((short)pFila["Prioridad"] < 11 || (short)pFila["Prioridad"] > 20)
            {
                if (numeroRecursos > 3000/* && proyID.Equals(ProyectoAD.ProyectoDidactalia)*/)
                {
                    BaseComunidadCN baseComunidadCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                    try
                    {
                        baseComunidadCN.InsertarFilaColaRefrescoCacheEnRabbitMQ(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.PersonasYOrganizaciones, null);
                    }
                    catch (Exception ex)
                    {
                        loggingService.GuardarLogError(ex, "Fallo al insertar en Rabbit, insertamos en la base de datos BASE, tabla colaRefrescoCache");
                        baseComunidadCN.InsertarFilaEnColaRefrescoCache(proyID, TiposEventosRefrescoCache.BusquedaVirtuoso, TipoBusqueda.PersonasYOrganizaciones);
                    }

                    baseComunidadCN.Dispose();
                }
                else
                {
                    FacetadoCL facetadoCL = new FacetadoCL(mFicheroConfiguracionBD, mFicheroConfiguracionBD, mUrlIntragnoss, entityContext, loggingService, redisCacheWrapper, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    facetadoCL.Dominio = mDominio;
                    facetadoCL.InvalidarResultadosYFacetasDeBusquedaEnProyecto(proyID, FacetadoAD.TipoBusquedaToString(TipoBusqueda.Recursos));
                    facetadoCL.BorrarRSSDeComunidad(proyID);
                    facetadoCL.Dispose();
                }
            }
            #endregion

        }

        #endregion

        protected Guid ObtenerElementoVinculadoIDDeDocumento(Guid pDocumentoID, Guid pProyectoID, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            DocumentacionCN docCN = new DocumentacionCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            DataWrapperDocumentacion docDW = docCN.ObtenerDocumentoDocumentoWebVinBRPorID(pDocumentoID, pProyectoID);
            docCN.Dispose();

            Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.Documento filaDocumento = docDW.ListaDocumento.FirstOrDefault(doc => doc.DocumentoID.Equals(pDocumentoID));
            Guid ontologiaID = Guid.Empty;
            if (!filaDocumento.ElementoVinculadoID.HasValue && (filaDocumento.Tipo.Equals((short)TiposDocumentacion.Ontologia) || filaDocumento.Tipo.Equals((short)TiposDocumentacion.OntologiaSecundaria)))
            {
                ontologiaID = pDocumentoID;
            }
            else
            {
                ontologiaID = filaDocumento.ElementoVinculadoID.Value;
            }

            return ontologiaID;
        }

        protected Ontologia ObtenerOntologia(Guid pOntologiaID, Guid pProyectoID, LoggingService loggingService, EntityContext entityContext, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, EntityContextBASE entityContextBASE, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            ControladorDocumentacion controladorDocumentacion = new ControladorDocumentacion(loggingService, entityContext, mConfigService, redisCacheWrapper, gnossCache, entityContextBASE, virtuosoAD, null, servicesUtilVirtuosoAndReplication);
            byte[] arrayOntologia = controladorDocumentacion.ObtenerOntologia(pOntologiaID, pProyectoID, null);

            //Leo la ontología:
            Ontologia ontologia = new Ontologia(arrayOntologia, true);
            ontologia.LeerOntologia();
            ontologia.OntologiaID = pOntologiaID;

            return ontologia;
        }

        protected void ObtenerListasTipoElementosFacetas(Guid pProyectoID, Guid pOrganizacionID, ref List<string> pFecha, ref List<string> pNumero, ref List<string> pTextoInvariable, EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetaCN tablasDeConfiguracionCN = new FacetaCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            DataWrapperFacetas configFacDW = tablasDeConfiguracionCN.ObtenerFacetaObjetoConocimientoProyecto(pOrganizacionID, pProyectoID);
            List<Es.Riam.Gnoss.AD.EntityModel.Models.Faceta.FacetaObjetoConocimientoProyecto> filas = configFacDW.ListaFacetaObjetoConocimientoProyecto.Where(item => item.ProyectoID.Equals(pProyectoID)).ToList();

            tablasDeConfiguracionCN.Dispose();

            foreach (Es.Riam.Gnoss.AD.EntityModel.Models.Faceta.FacetaObjetoConocimientoProyecto myrow in filas)
            {

                if (myrow.TipoPropiedad.Equals((short)TipoPropiedadFaceta.Fecha) || myrow.TipoPropiedad.Equals((short)TipoPropiedadFaceta.Calendario) || myrow.TipoPropiedad.Equals((short)TipoPropiedadFaceta.CalendarioConRangos) || myrow.TipoPropiedad.Equals((short)TipoPropiedadFaceta.Siglo))
                {
                    string fechaAux = myrow.Faceta;
                    string propiedadFecha = fechaAux.Substring(fechaAux.LastIndexOf(":") + 1);

                    if (!pFecha.Contains(propiedadFecha))
                    {
                        pFecha.Add(propiedadFecha);
                    }
                }

                if (myrow.TipoPropiedad.Equals((short)TipoPropiedadFaceta.Numero))
                {
                    string numeroAux = myrow.Faceta;
                    string propiedadNumero = numeroAux.Substring(numeroAux.LastIndexOf(":") + 1);

                    if (!pNumero.Contains(propiedadNumero))
                    {
                        pNumero.Add(propiedadNumero);
                    }
                }

                if (myrow.TipoPropiedad.Equals((short)TipoPropiedadFaceta.TextoInvariable))
                {
                    string facetaAux = myrow.Faceta;
                    pTextoInvariable.Add(facetaAux.Substring(facetaAux.LastIndexOf(":") + 1));
                }
            }
        }

        /// <summary>
        /// Actualiza el número de comentarios de virtuoso.
        /// </summary>
        protected void ActualizarNumComentariosVirtuoso(Guid pComentarioID, EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            DocumentacionCN docCN = new DocumentacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            Guid documentoID = docCN.ObtenerIDDocumentoDeComentarioPorID(pComentarioID);
            DataWrapperDocumentacion docDW = docCN.ObtenerDocumentoPorID(documentoID);
            docCN.Dispose();

            if (docDW.ListaDocumento.Count(doc => doc.DocumentoID.Equals(documentoID)) > 0)
            {
                //Actualizamos el numero de comentarios que tiene el documento en el proyecto.
                Guid proyectoID = docDW.ListaDocumento.First(doc => doc.DocumentoID.Equals(documentoID)).ProyectoID.Value;

                ModificarVotosVisitasComentarios_ControlCheckPoint(proyectoID, documentoID, "Comentarios", loggingService, entityContext, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);

                //Actualizamos el número de comentarios que ha hecho la identidad "comentarista"
                List<Guid> listComentarios = new List<Guid>();
                listComentarios.Add(pComentarioID);
                ComentarioCN comCN = new ComentarioCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                DataWrapperComentario comDW = comCN.ObtenerComentariosPorID(listComentarios);
                comCN.Dispose();

                if (comDW.ListaComentario.Where(item => item.ComentarioID.Equals(pComentarioID)).Count() > 0)
                {
                    Guid identidadID = comDW.ListaComentario.Where(item => item.ComentarioID.Equals(pComentarioID)).FirstOrDefault().IdentidadID;

                    IdentidadCN identCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                    DataWrapperIdentidad identDW = identCN.ObtenerIdentidadPorID(identidadID, true);
                    identCN.Dispose();

                    List<Es.Riam.Gnoss.AD.EntityModel.Models.IdentidadDS.Identidad> listaIdentidad = identDW.ListaIdentidad.Where(identidad => identidad.IdentidadID.Equals(identidadID)).ToList();
                    if (listaIdentidad.Count > 0)
                    {
                        Guid perfilID = listaIdentidad.First().PerfilID;

                        ModificarVotosVisitasComentarios_ControlCheckPoint(perfilID, documentoID, "Comentarios", loggingService, entityContext, virtuosoAD, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                        //facetadoCN.ModificarVotosVisitasComentarios(perfilID.ToString(), documentoID.ToString(), "Comentarios");

                    }
                }
            }
        }

        protected string PasarQueryTriplesATriples(List<QueryTriples> pQueryTriples)
        {
            string tripes = "";
            foreach (QueryTriples query in pQueryTriples)
            {
                tripes += FacetadoAD.GenerarTripleta(query.Sujeto, query.Predicado, query.Objeto);
            }
            return tripes;
        }

        protected string PasarDataSetATriples(DataSet pDataSet)
        {
            string tripes = "";
            foreach (DataRow fila in pDataSet.Tables[0].Rows)
            {
                tripes += FacetadoAD.GenerarTripleta("<http://gnoss/" + fila[0].ToString().ToUpper() + ">", "<" + fila[1] + ">", "<http://gnoss/" + fila[2].ToString().ToUpper() + ">");
            }
            return tripes;
        }

        /// <summary>
        /// Pasa una cadena de texto a UTF8.
        /// </summary>
        /// <param name="cadena">Cadena</param>
        /// <returns>cadena de texto en UTF8</returns>
        public static string PasarAUtf8(string cadena)
        {
            Encoding EncodingANSI = Encoding.GetEncoding("iso8859-1");
            return EncodingANSI.GetString(Encoding.UTF8.GetBytes(cadena));
        }

        protected string GenerarTripletaRecogidadeVirtuoso_ControlCheckPoint(string pSujeto, string pPredicado, string pObjeto, string pObjetoSinMinuscula, List<string> Fecha, List<string> Numero, List<string> TextoInvariable, List<Es.Riam.Gnoss.AD.EntityModel.Models.Faceta.FacetaEntidadesExternas> EntExt, UtilidadesVirtuoso utilidadesVirtuoso)
        {
            return utilidadesVirtuoso.GenerarTripletaRecogidadeVirtuoso_ControlCheckPoint(mFicheroConfiguracionBD, mFicheroConfiguracionBDBase, mUrlIntragnoss, pSujeto, pPredicado, pObjeto, pObjetoSinMinuscula, Fecha, Numero, TextoInvariable, EntExt, null);
        }

        #endregion

        #region ControlCheckPoint

        /// <summary>
        /// Control de insercciones en virtuoso en las horas del checkpoint.
        /// </summary>
        /// <param name="pFacetadoAD">Controlador de virtuoso AD.</param>
        /// <param name="pProyectoID">ProyectoID</param>
        /// <param name="pSujeto">Sujeto de la triple</param>
        /// <param name="pPredicado">Predicado de la triple</param>
        /// <param name="pObjeto">Objeto de la triple</param>
        protected void BorrarTripleta_ControlCheckPoint(string pProyectoID, string pSujeto, string pPredicado, string pObjeto, EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetadoAD facetadoAD = null;

            try
            {
                facetadoAD = new FacetadoAD(mFicheroConfiguracionBD, mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                facetadoAD.BorrarTripleta(pProyectoID, pSujeto, pPredicado, pObjeto);
            }
            catch (Exception ex)
            {
                //Cerramos las conexiones
                ControladorConexiones.CerrarConexiones(false);

                //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                while (!utilidadesVirtuoso.ServidorOperativo(mFicheroConfiguracionBD, mUrlIntragnoss))
                {
                    //Dormimos 30 segundos
                    Thread.Sleep(30 * 1000);
                }

                facetadoAD = new FacetadoAD(mFicheroConfiguracionBD, mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                facetadoAD.BorrarTripleta(pProyectoID, pSujeto, pPredicado, pObjeto);
            }
            finally
            {
                facetadoAD.Dispose();
                facetadoAD = null;
            }
        }

        /// <summary>
        /// Control de insercciones en virtuoso en las horas del checkpoint.
        /// </summary>
        /// <param name="pPrioridadFila">Prioridad de las tripletas.</param>
        /// <param name="pGrafo"></param>
        /// <param name="pTripletas">Tripletas a insertar</param>
        /// <param name="pPrioridad">Prioridad de las tripletas.</param>
        protected void InsertarTripletas_ControlCheckPoint(int pPrioridadFila, string pGrafo, string pTripletas, int pPrioridad, EntityContext entityContext, LoggingService loggingService, VirtuosoAD virtuosoAD, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetadoAD facetadoAD = null;
            if (mEscribirFicheroExternoTriples && pPrioridadFila == 5)
            {
                try
                {
                    facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                    string grafoFinal = facetadoAD.ObtenerUrlGrafo(pGrafo).Replace("<", "").Replace(">", "").Trim();

                    //Guardamos los datos en un fichero de tripletas
                    this.EscribirFichero(pTripletas, grafoFinal, ".ttl", loggingService);
                }
                catch (Exception ex) { throw; }
                finally
                {
                    facetadoAD.Dispose();
                    facetadoAD = null;
                }
            }
            else
            {
                try
                {
                    facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                    facetadoAD.InsertaTripletas(pGrafo, pTripletas, (short)pPrioridad, false);
                }
                catch (Exception ex)
                {
                    //Cerramos las conexiones
                    ControladorConexiones.CerrarConexiones(false);

                    //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                    while (!utilidadesVirtuoso.ServidorOperativo(mFicheroConfiguracionBD, mUrlIntragnoss))
                    {
                        //Dormimos 30 segundos
                        Thread.Sleep(30 * 1000);
                    }

                    facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;
                    facetadoAD.InsertaTripletas(pGrafo, pTripletas, (short)pPrioridad);
                }
                finally
                {
                    facetadoAD.Dispose();
                    facetadoAD = null;
                }
            }
        }

        /// <summary>
        /// Control de insercciones en virtuoso en las horas del checkpoint.
        /// </summary>
        /// <param name="pFacetadoAD">Controlador de virtuoso AD.</param>
        /// <param name="pProyectoID">ProyectoID</param>
        /// <param name="pTripletas">Tripletas a insertar</param>
        /// <param name="pListaElementosaModificarID"></param>
        /// <param name="pCondicionesWhere"></param>
        /// <param name="pCondicionesFilter"></param>
        protected void InsertaTripletasConModify_ControlCheckPoint(int pPrioridadFila, string pGrafo, string pTripletas, Dictionary<string, string> pListaElementosaModificarID, string pCondicionesWhere, string pCondicionesFilter, bool pEsDocSemantico, LoggingService loggingService, EntityContext entityContext, VirtuosoAD virtuosoAD, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetadoAD facetadoAD = null;
            if (mEscribirFicheroExternoTriples && pPrioridadFila == 5)
            {
                try
                {
                    facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                    string grafoFinal = facetadoAD.ObtenerUrlGrafo(pGrafo).Replace("<", "").Replace(">", "").Trim();

                    //Guardamos los datos en un fichero de tripletas
                    this.EscribirFichero(pTripletas, grafoFinal, ".ttl", loggingService);
                }
                catch (Exception ex) { throw; }
                finally
                {
                    facetadoAD.Dispose();
                    facetadoAD = null;
                }
            }
            else
            {
                try
                {
                    facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                    facetadoAD.InsertaTripletasConModify(pGrafo, pTripletas, pListaElementosaModificarID, pCondicionesWhere, pCondicionesFilter, true, 0, pEsDocSemantico);
                }
                catch (Exception ex)
                {
                    //Cerramos las conexiones
                    ControladorConexiones.CerrarConexiones(false);

                    //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                    while (!utilidadesVirtuoso.ServidorOperativo(mFicheroConfiguracionBD, mUrlIntragnoss))
                    {
                        //Dormimos 30 segundos
                        Thread.Sleep(30 * 1000);
                    }

                    facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;
                    facetadoAD.InsertaTripletasConModify(pGrafo, pTripletas, pListaElementosaModificarID, pCondicionesWhere, pCondicionesFilter, true, 0, pEsDocSemantico);
                }
                finally
                {
                    facetadoAD.Dispose();
                    facetadoAD = null;
                }
            }
        }

        protected void ObtenerIDDocCVDesdeVirtuoso_ControlCheckPoint(FacetadoDS pFacetadoCVDS, string pProyid, string pId, LoggingService loggingService, EntityContext entityContext, VirtuosoAD virtuosoAD, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetadoAD facetadoAD = null;
            try
            {
                facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                facetadoAD.ObtenerIDDocCVDesdeVirtuoso(pFacetadoCVDS, pProyid, pId);
            }
            catch (Exception ex)
            {
                //Cerramos las conexiones
                ControladorConexiones.CerrarConexiones(false);

                //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                while (!utilidadesVirtuoso.ServidorOperativo(mFicheroConfiguracionBD, mUrlIntragnoss))
                {
                    //Dormimos 30 segundos
                    Thread.Sleep(30 * 1000);
                }

                facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;
                facetadoAD.ObtenerIDDocCVDesdeVirtuoso(pFacetadoCVDS, pProyid, pId);
            }
            finally
            {
                facetadoAD.Dispose();
                facetadoAD = null;
            }
        }

        protected FacetadoDS ObtieneElementosDeCategoria_ControlCheckPoint(string pIdProy, string pCatEliminar, LoggingService loggingService, EntityContext entityContext, VirtuosoAD virtuosoAD, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetadoDS facDS = new FacetadoDS();

            FacetadoAD facetadoAD = null;
            try
            {
                facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                facDS = facetadoAD.ObtieneElementosDeCategoria(pIdProy, pCatEliminar);
            }
            catch (Exception ex)
            {
                //Cerramos las conexiones
                ControladorConexiones.CerrarConexiones(false);

                //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                while (!utilidadesVirtuoso.ServidorOperativo(mFicheroConfiguracionBD, mUrlIntragnoss))
                {
                    //Dormimos 30 segundos
                    Thread.Sleep(30 * 1000);
                }

                facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;
                facDS = facetadoAD.ObtieneElementosDeCategoria(pIdProy, pCatEliminar);
            }
            finally
            {
                facetadoAD.Dispose();
                facetadoAD = null;
            }

            return facDS;
        }

        protected void ModificarVotosVisitasComentarios_ControlCheckPoint(Guid pProyectoID, Guid pDocumentoID, string pGrafo, LoggingService loggingService, EntityContext entityContext, VirtuosoAD virtuosoAD, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetadoAD facetadoAD = null;
            try
            {

                facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                facetadoAD.ModificarVotosVisitasComentarios(pProyectoID.ToString(), pDocumentoID.ToString(), pGrafo, 1);
            }
            catch (Exception ex)
            {
                //Cerramos las conexiones
                ControladorConexiones.CerrarConexiones(false);

                //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                while (!utilidadesVirtuoso.ServidorOperativo(mFicheroConfiguracionBD, mUrlIntragnoss))
                {
                    //Dormimos 30 segundos
                    Thread.Sleep(30 * 1000);
                }
                facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                facetadoAD.ModificarVotosVisitasComentarios(pProyectoID.ToString(), pDocumentoID.ToString(), pGrafo, 1);
            }
            finally
            {
                facetadoAD.Dispose();
                facetadoAD = null;
            }
        }

        protected Ontologia CargarOntologia(Guid pDocumentoID, Guid pProyectoID, LoggingService loggingService, EntityContext entityContext, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, EntityContextBASE entityContextBASE, VirtuosoAD virtuosoAD, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            Guid ontologiaID = ObtenerElementoVinculadoIDDeDocumento(pDocumentoID, pProyectoID, entityContext, loggingService, servicesUtilVirtuosoAndReplication);

            if (!mListaOntologiasPorID.ContainsKey(ontologiaID))
            {
                mListaOntologiasPorID.Add(ontologiaID, ObtenerOntologia(ontologiaID, pProyectoID, loggingService, entityContext, redisCacheWrapper, gnossCache, entityContextBASE, virtuosoAD, servicesUtilVirtuosoAndReplication));

                mListaElementosContenedorSuperiorOHerencias.Add(ontologiaID, GestionOWL.ObtenerElementosContenedorSuperiorOHerencias(mListaOntologiasPorID[ontologiaID].Entidades));
            }
            return mListaOntologiasPorID[ontologiaID];
        }

        protected string ObtenerTriplesFormularioSemantico_ControlCheckPoint(string pFicheroConfiguracionBD, string pFicheroConfiguracionBDBase, string pUrlIntragnoss, DataWrapperFacetas pConfiguracion, Guid pOrganizacionID, Guid pProyID, Guid pDocumentoID, out string pTypeSem, LoggingService loggingService, EntityContext entityContext, RedisCacheWrapper redisCacheWrapper, GnossCache gnossCache, EntityContextBASE entityContextBASE, VirtuosoAD virtuosoAD, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            Ontologia ontologia = CargarOntologia(pDocumentoID, pProyID, loggingService, entityContext, redisCacheWrapper, gnossCache, entityContextBASE, virtuosoAD, servicesUtilVirtuosoAndReplication);

            string triples = "";
            try
            {

                triples = utilidadesVirtuoso.ObtenerTriplesFormularioSemantico(pFicheroConfiguracionBD, pFicheroConfiguracionBDBase, pUrlIntragnoss, pConfiguracion, pOrganizacionID, pProyID, pDocumentoID, ontologia, DicPropiedadesOntologia, out pTypeSem, mListaElementosContenedorSuperiorOHerencias[ontologia.OntologiaID]);
            }
            catch (Exception ex)
            {
                //Cerramos las conexiones
                ControladorConexiones.CerrarConexiones(false);

                //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                while (!utilidadesVirtuoso.ServidorOperativo(mFicheroConfiguracionBD, mUrlIntragnoss))
                {
                    //Dormimos 30 segundos
                    Thread.Sleep(30 * 1000);
                }

                triples = utilidadesVirtuoso.ObtenerTriplesFormularioSemantico(pFicheroConfiguracionBD, pFicheroConfiguracionBDBase, pUrlIntragnoss, pConfiguracion, pOrganizacionID, pProyID, pDocumentoID, ontologia, DicPropiedadesOntologia, out pTypeSem, mListaElementosContenedorSuperiorOHerencias[ontologia.OntologiaID]);
            }

            return triples;
        }

        protected string ObtenerValoresSemanticosSearch_ControlCheckPoint(string pFicheroConfiguracionBD, string pUrlIntragnoss, Guid pProyID, Guid pDocumentoID, UtilidadesVirtuoso utilidadesVirtuoso)
        {
            string triples = "";
            try
            {
                triples = utilidadesVirtuoso.ObtenerValoresSemanticosSearch(pFicheroConfiguracionBD, pUrlIntragnoss, pProyID, pDocumentoID);
            }
            catch (Exception ex)
            {
                //Cerramos las conexiones
                ControladorConexiones.CerrarConexiones(false);

                //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                while (!utilidadesVirtuoso.ServidorOperativo(mFicheroConfiguracionBD, mUrlIntragnoss))
                {
                    //Dormimos 30 segundos
                    Thread.Sleep(30 * 1000);
                }

                triples = utilidadesVirtuoso.ObtenerValoresSemanticosSearch(pFicheroConfiguracionBD, pUrlIntragnoss, pProyID, pDocumentoID);
            }

            return triples;
        }

        protected void ActualizarPublicadorEditorRecursosComunidad_ControlCheckPoint(Guid pProyID, Guid pIdentidadID, string pNombrePersona, int pTablaBaseProyectoID, LoggingService loggingService, EntityContext entityContext, VirtuosoAD virtuosoAD, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetadoAD facetadoAD = null;

            try
            {
                if (!string.IsNullOrEmpty(pNombrePersona))
                {
                    facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;

                    ReprocesarRecursosConMasDeUnEditor(facetadoAD, false, pProyID, pIdentidadID, pTablaBaseProyectoID, pNombrePersona, entityContext, loggingService, entityContextBASE, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
                }
            }
            catch (Exception ex)
            {
                //Cerramos las conexiones
                ControladorConexiones.CerrarConexiones(false);

                //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                while (!utilidadesVirtuoso.ServidorOperativo(mFicheroConfiguracionBD, mUrlIntragnoss))
                {
                    //Dormimos 30 segundos
                    Thread.Sleep(30 * 1000);
                }

                ReprocesarRecursosConMasDeUnEditor(facetadoAD, false, pProyID, pIdentidadID, pTablaBaseProyectoID, pNombrePersona, entityContext, loggingService, entityContextBASE, utilidadesVirtuoso, servicesUtilVirtuosoAndReplication);
            }
            finally
            {
                facetadoAD.Dispose();
                facetadoAD = null;
            }
        }

        protected void ActualizarGrupoLectorEditorRecursosComunidad_ControlCheckPoint(Guid pProyID, string pNombreGrupoViejo, string pNombreGrupoNuevo, int pTablaBaseProyectoID, LoggingService loggingService, EntityContext entityContext, VirtuosoAD virtuosoAD, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            FacetadoAD facetadoAD = null;

            try
            {
                if (!string.IsNullOrEmpty(pNombreGrupoNuevo))
                {
                    facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;
                    facetadoAD.ActualizarGrupoEditorRecursos(false, pProyID, pNombreGrupoViejo, pNombreGrupoNuevo);
                    facetadoAD.ActualizarGrupoLectorRecursos(false, pProyID, pNombreGrupoViejo, pNombreGrupoNuevo);
                }
            }
            catch (Exception ex)
            {
                //Cerramos las conexiones
                ControladorConexiones.CerrarConexiones(false);

                //Realizamos una consulta ask a virtuoso para comprobar si está funcionando
                while (!utilidadesVirtuoso.ServidorOperativo(mFicheroConfiguracionBD, mUrlIntragnoss))
                {
                    //Dormimos 30 segundos
                    Thread.Sleep(30 * 1000);
                }

                if (!string.IsNullOrEmpty(pNombreGrupoNuevo))
                {
                    facetadoAD = new FacetadoAD(mUrlIntragnoss, loggingService, entityContext, mConfigService, virtuosoAD, servicesUtilVirtuosoAndReplication);
                    facetadoAD.CadenaConexionBase = this.mFicheroConfiguracionBDBase;
                    facetadoAD.ActualizarGrupoEditorRecursos(false, pProyID, pNombreGrupoViejo, pNombreGrupoNuevo);
                    facetadoAD.ActualizarGrupoLectorRecursos(false, pProyID, pNombreGrupoViejo, pNombreGrupoNuevo);
                }
            }
            finally
            {
                facetadoAD.Dispose();
                facetadoAD = null;
            }
        }

        protected void ReprocesarRecursosConMasDeUnEditor(FacetadoAD pFacetadoAD, bool pUsarColaActualizacion, Guid pProyID, Guid pIdentidadID, int pTablaBaseProyectoID, string pNombrePersona, EntityContext entityContext, LoggingService loggingService, EntityContextBASE entityContextBASE, UtilidadesVirtuoso utilidadesVirtuoso, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            IdentidadCN identCN = new IdentidadCN(entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            Guid? perfilID = identCN.ObtenerPerfilIDDeIdentidadID(pIdentidadID);
            identCN.Dispose();

            //Por cada recurso de la comunidad en el que la identidadid sea editor, y haya más de una fila de editores, hay que enviar al base recursos una fila para que lo re-procese.
            DocumentacionCN docCN = new DocumentacionCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
            List<Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.Documento> listaDocumento = docCN.ObtenerRecursosIdentidadProyectoEditor(pProyID, perfilID.Value, false);

            if (listaDocumento.Count > 0)
            {
                //Documentos con más de un editor en la comunidad.

                //Obtener todos los documentos que tienen como único editor a la identidad
                List<Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.Documento> listaDocUnicoEditor = docCN.ObtenerRecursosIdentidadProyectoEditor(pProyID, perfilID.Value, true);
                List<Guid> listaRecursosUnicoEditor = new List<Guid>();
                int i = 0;
                foreach (Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.Documento docSoloUnEditor in listaDocUnicoEditor)
                {
                    //Modificamos de 100 en 100
                    if (i >= 100)
                    {
                        utilidadesVirtuoso.ActualizarPublicadorEditorRecursosComunidad(pFacetadoAD, mFicheroConfiguracionBD, mUrlIntragnoss, pUsarColaActualizacion, pProyID, pIdentidadID, pNombrePersona, listaRecursosUnicoEditor);
                        i = 0;
                        listaRecursosUnicoEditor.Clear();
                    }
                    i++;

                    listaRecursosUnicoEditor.Add(docSoloUnEditor.DocumentoID);
                }

                if (listaRecursosUnicoEditor.Count > 0)
                {
                    //No debe reprocesar los editores si no hay ninguno en la lista.
                    //La identidad que se ha cambiado pertenece a la comunidad, pero no ha creado ningún recurso, simplemente ha sido seleccionada como editora de algún recurso y no debe entrar por aquí.
                    utilidadesVirtuoso.ActualizarPublicadorEditorRecursosComunidad(pFacetadoAD, mFicheroConfiguracionBD, mUrlIntragnoss, pUsarColaActualizacion, pProyID, pIdentidadID, pNombrePersona, listaRecursosUnicoEditor);
                    i = 0;
                    listaRecursosUnicoEditor.Clear();
                }
                listaDocUnicoEditor = null;

                //Reprocesar por el base el resto de documentos con más de un editor
                BaseRecursosComunidadDS baseRecursosDS = new BaseRecursosComunidadDS();
                foreach (Es.Riam.Gnoss.AD.EntityModel.Models.Documentacion.Documento docMasDeUnEditor in listaDocumento)
                {
                    BaseRecursosComunidadDS.ColaTagsComunidadesRow colaTagsComunidades = baseRecursosDS.ColaTagsComunidades.NewColaTagsComunidadesRow();
                    colaTagsComunidades.TablaBaseProyectoID = pTablaBaseProyectoID;
                    colaTagsComunidades.Tags = "##ID_TAG_DOC##" + docMasDeUnEditor.DocumentoID + "##ID_TAG_DOC##,##TIPO_DOC##" + docMasDeUnEditor.Tipo + "##TIPO_DOC##," + utilidadesVirtuoso.TagBaseAfinidadVirtuoso;
                    colaTagsComunidades.Tipo = 0;
                    colaTagsComunidades.Estado = 0;
                    colaTagsComunidades.FechaPuestaEnCola = DateTime.Now;
                    colaTagsComunidades.Prioridad = 1;
                    baseRecursosDS.ColaTagsComunidades.AddColaTagsComunidadesRow(colaTagsComunidades);
                }

                BaseComunidadCN brDafoCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, pTablaBaseProyectoID, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
                brDafoCN.InsertarFilasEnRabbit("ColaTagsComunidades", baseRecursosDS);
                brDafoCN.Dispose();
            }
            else
            {
                //No hay documentos con editores para esa identidad, modificar todos los editores en virtuoso.
                utilidadesVirtuoso.ActualizarPublicadorEditorRecursosComunidad(pFacetadoAD, mFicheroConfiguracionBD, mUrlIntragnoss, pUsarColaActualizacion, pProyID, pIdentidadID, pNombrePersona, null);
            }

            docCN.Dispose();
        }

        #endregion

        #region Utilidades tags

        /// <summary>
        /// Separa los tags en una lista
        /// </summary>
        /// <param name="pTags">tags a separar</param>
        /// <param name="pListaTagsDirectos">Lista de tags directos inicializada</param>
        /// <param name="pListaTagsIndirectos">Lista de tags indicrectos inicializada</param>
        /// <param name="pListaTagsFiltros">Lista de tags que provienen de filtros</param>
        /// <param name="pDataSet">Data set de la fila de cola</param>
        /// <returns>Lista de todos los tags</returns>
        protected List<string> SepararTags(string pTags, List<string> pListaTagsDirectos, List<string> pListaTagsIndirectos, Dictionary<short, List<string>> pListaTagsFiltros, DataSet pDataSet)
        {
            List<string> lista = new List<string>();

            if (!string.IsNullOrEmpty(pTags))
            {
                ObtenerTagsFiltros(ref pTags, pListaTagsFiltros, lista, pDataSet);
                char[] seps = { ',' };
                string[] tags = pTags.Split(seps, StringSplitOptions.RemoveEmptyEntries);

                foreach (string tag in tags)
                {
                    string tagDirecto = tag.Trim().ToLower();

                    if (!string.IsNullOrEmpty(tagDirecto))
                    {
                        pListaTagsDirectos.Add(tagDirecto);

                        //Obtengo los tags indirectos
                        int palabrasDescartadas = 0;
                        List<string> listaTagsDescompuestos = AnalizadorSintactico.ObtenerTagsFrase(tagDirecto, out palabrasDescartadas);

                        //if ((palabrasDescartadas + listaTagsDescompuestos.Count) > 1)
                        {
                            foreach (string tagIndirecto in listaTagsDescompuestos)
                            {
                                if ((!pListaTagsDirectos.Contains(tagIndirecto)) && (!pListaTagsIndirectos.Contains(tagIndirecto)))
                                {
                                    //Si el posible tag indirecto no es directo, lo añado
                                    pListaTagsIndirectos.Add(tagIndirecto);
                                }
                            }
                        }
                    }
                }
            }
            lista.AddRange(pListaTagsDirectos);
            lista.AddRange(pListaTagsIndirectos);

            return lista;
        }

        /// <summary>
        /// Busca un filtro concreto en una cadena
        /// </summary>
        /// <param name="pCadena">Cadena en la que se debe buscar</param>
        /// <param name="pClaveFiltro">Clave del filtro (##CAT_DOC##, ...)</param>
        /// <returns></returns>
        protected List<string> BuscarTagFiltroEnCadena(ref string pCadena, string pClaveFiltro)
        {
            string filtro = "";
            List<string> listaFiltros = new List<string>();

            int indiceFiltro = pCadena.IndexOf(pClaveFiltro);

            if (indiceFiltro >= 0)
            {
                string subCadena = pCadena.Substring(indiceFiltro + pClaveFiltro.Length);

                filtro = subCadena.Substring(0, subCadena.IndexOf(pClaveFiltro));

                if ((pClaveFiltro.Equals(Constantes.TIPO_DOC)) || (pClaveFiltro.Equals(Constantes.PERS_U_ORG)) || (pClaveFiltro.Equals(Constantes.ESTADO_COMENTADO)))
                {
                    //Estos tags van con la clave del tag (para tags de tipo entero o similar, ej: Tipos de documento, para que al buscar '0' no aparezcan los tags de todos los recursos que son de tal tipo). 
                    filtro = pClaveFiltro + filtro + pClaveFiltro;
                    pCadena = pCadena.Replace(filtro, "");
                }
                else
                {
                    pCadena = pCadena.Replace(pClaveFiltro + filtro + pClaveFiltro, "");

                    if (!pClaveFiltro.Equals(Constantes.AFINIDAD_VIRTUOSO))
                    {
                        filtro = filtro.ToLower();
                    }
                }
                if (filtro.Trim() != "")
                {
                    listaFiltros.Add(filtro);
                }
                listaFiltros.AddRange(BuscarTagFiltroEnCadena(ref pCadena, pClaveFiltro));
            }
            return listaFiltros;
        }

        #endregion

        #region METODOS CONFIGURABLES para recursos, personas, organizaciones...

        #region Utilidades de tags

        /// <summary>
        /// Comprueba si un tag proviene de un filtro
        /// </summary>
        /// <param name="pTags">Cadena que contiene los tags</param>
        /// <param name="pListaTagsFiltros">Lista de tags que provienen de filtros</param>
        /// <param name="pListaTodosTags">Lista de todos los tags</param>
        /// <param name="pDataSet">Data set de la fila de cola</param>
        /// <returns></returns>
        protected void ObtenerTagsFiltros(ref string pTags, Dictionary<short, List<string>> pListaTagsFiltros, List<string> pListaTodosTags, DataSet pDataSet)
        {
            if (pDataSet is BaseRecursosComunidadDS)
            {
                //RECURSOS

                //Recurso o Cometnario
                pListaTagsFiltros.Add((short)TiposTags.ComentarioORecurso, BuscarTagFiltroEnCadena(ref pTags, Constantes.COM_U_REC));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.ComentarioORecurso]);


                //Autor del documento
                pListaTagsFiltros.Add((short)TiposTags.AutorDocumento, BuscarTagFiltroEnCadena(ref pTags, Constantes.AUT_DOC));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.AutorDocumento]);

                //Categoría del tesauro del documento
                pListaTagsFiltros.Add((short)TiposTags.CategoriaTesauro, BuscarTagFiltroEnCadena(ref pTags, Constantes.CAT_DOC));
                //Esta linea se hace abajo, con las categorías de las comunidades:
                //pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.CategoriaTesauro]);

                //Extensión del documento
                pListaTagsFiltros.Add((short)TiposTags.ExtensionDocumento, BuscarTagFiltroEnCadena(ref pTags, Constantes.EXT_DOC));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.ExtensionDocumento]);

                //Fecha de publicación del documento
                pListaTagsFiltros.Add((short)TiposTags.FechaPublicacionDocumento, BuscarTagFiltroEnCadena(ref pTags, Constantes.FECHAPUB_DOC));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.FechaPublicacionDocumento]);

                //Nivel de certificación del documento
                pListaTagsFiltros.Add((short)TiposTags.NivelCertificacionDocumento, BuscarTagFiltroEnCadena(ref pTags, Constantes.NIVCER_DOC));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.NivelCertificacionDocumento]);

                //Publicador del documento
                pListaTagsFiltros.Add((short)TiposTags.PublicadorDocumento, BuscarTagFiltroEnCadena(ref pTags, Constantes.PUB_DOC));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.PublicadorDocumento]);

                //Nombre del documento
                pListaTagsFiltros.Add((short)TiposTags.DocumentoNombre, BuscarTagFiltroEnCadena(ref pTags, Constantes.ENLACE_DOC));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.DocumentoNombre]);

                //Titulo del documento
                pListaTagsFiltros.Add((short)TiposTags.DocumentoTitulo, BuscarTagFiltroEnCadena(ref pTags, Constantes.NOMBRE_DOC));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.DocumentoTitulo]);

                //Tipo del documento
                pListaTagsFiltros.Add((short)TiposTags.TipoDocumento, BuscarTagFiltroEnCadena(ref pTags, Constantes.TIPO_DOC));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.TipoDocumento]);

                //Estado comentado del documento
                pListaTagsFiltros.Add((short)TiposTags.EstadoComentado, BuscarTagFiltroEnCadena(ref pTags, Constantes.ESTADO_COMENTADO));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.EstadoComentado]);

                //ID-Tag del documento
                pListaTagsFiltros.Add((short)TiposTags.IDTagDoc, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_TAG_DOCUMENTO));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.IDTagDoc]);
            }
            else if (pDataSet is BasePerOrgComunidadDS)
            {
                //PERSONAS

                //Nombre completo de la persona
                pListaTagsFiltros.Add((short)TiposTags.PersonaNombreCompleto, BuscarTagFiltroEnCadena(ref pTags, Constantes.NOMBRE_PER_COMPLETO));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.PersonaNombreCompleto]);

                //Nombre de la persona
                pListaTagsFiltros.Add((short)TiposTags.PersonaNombreSinApellidos, BuscarTagFiltroEnCadena(ref pTags, Constantes.NOMBRE_PER_SIN_APP));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.PersonaNombreSinApellidos]);

                //Apellidos de la persona
                pListaTagsFiltros.Add((short)TiposTags.PersonaApellidos, BuscarTagFiltroEnCadena(ref pTags, Constantes.APELLIDOS_PER));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.PersonaApellidos]);

                //País de la persona
                pListaTagsFiltros.Add((short)TiposTags.PersonaPais, BuscarTagFiltroEnCadena(ref pTags, Constantes.PAIS_PER));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.PersonaPais]);

                //Provincia de la persona
                pListaTagsFiltros.Add((short)TiposTags.PersonaProvincia, BuscarTagFiltroEnCadena(ref pTags, Constantes.PROVINCIA_PER));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.PersonaProvincia]);

                //Comunidades de la persona
                pListaTagsFiltros.Add((short)TiposTags.PersonaParticipaComunidad, BuscarTagFiltroEnCadena(ref pTags, Constantes.COMUNIDAD_PER));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.PersonaParticipaComunidad]);
                //Persona u organización
                pListaTagsFiltros.Add((short)TiposTags.OrganizacionOPersona, BuscarTagFiltroEnCadena(ref pTags, Constantes.PERS_U_ORG));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.OrganizacionOPersona]);

                //ID-Tag del persona
                pListaTagsFiltros.Add((short)TiposTags.IDTagPer, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_TAG_PER));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.IDTagPer]);

                //ORGANIZACIONES

                //Nombre de la organizacion
                pListaTagsFiltros.Add((short)TiposTags.OrganizacionNombre, BuscarTagFiltroEnCadena(ref pTags, Constantes.NOMBRE_ORG));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.OrganizacionNombre]);

                //Tipo de la organizacion
                pListaTagsFiltros.Add((short)TiposTags.OrganizacionTipo, BuscarTagFiltroEnCadena(ref pTags, Constantes.TIPO_ORG));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.OrganizacionTipo]);

                //Sector de la organizacion
                pListaTagsFiltros.Add((short)TiposTags.OrganizacionSector, BuscarTagFiltroEnCadena(ref pTags, Constantes.SEC_ORG));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.OrganizacionSector]);

                //URL de la organizacion
                pListaTagsFiltros.Add((short)TiposTags.OrganizacionURL, BuscarTagFiltroEnCadena(ref pTags, Constantes.URL_ORG));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.OrganizacionURL]);

                //Pais de la organizacion
                pListaTagsFiltros.Add((short)TiposTags.OrganizacionPais, BuscarTagFiltroEnCadena(ref pTags, Constantes.PAIS_ORG));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.OrganizacionPais]);

                //Provincia de la organizacion
                pListaTagsFiltros.Add((short)TiposTags.OrganizacionProvincia, BuscarTagFiltroEnCadena(ref pTags, Constantes.PROVINCIA_ORG));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.OrganizacionProvincia]);

                //Provincia de la organizacion
                pListaTagsFiltros.Add((short)TiposTags.OrganizacionAlias, BuscarTagFiltroEnCadena(ref pTags, Constantes.ALIAS_ORG));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.OrganizacionAlias]);

                //Comunidades de la organizacion
                pListaTagsFiltros.Add((short)TiposTags.OrganizacionParticipaComunidad, BuscarTagFiltroEnCadena(ref pTags, Constantes.COMUNIDAD_ORG));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.OrganizacionParticipaComunidad]);

                //Tag descompuesto
                pListaTagsFiltros.Add((short)TiposTags.TagCVDescompuesto, BuscarTagFiltroEnCadena(ref pTags, Constantes.TAG_DESCOMPUESTO));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.TagCVDescompuesto]);
            }
            else if (pDataSet is BaseProyectosDS)
            {
                //Tags de proyectos
                pListaTagsFiltros.Add((short)TiposTags.ProyectoNombre, BuscarTagFiltroEnCadena(ref pTags, Constantes.COM_O_US));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.ProyectoNombre]);

                //ID-Tag del proyecto
                pListaTagsFiltros.Add((short)TiposTags.IDTagProy, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_TAG_PROY));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.IDTagProy]);
            }
            else if (pDataSet is BasePaginaCMSDS)
            {
                //Proyecto o Usuario
                pListaTagsFiltros.Add((short)TiposTags.IDPestanyaCMSProyecto, BuscarTagFiltroEnCadena(ref pTags, Constantes.ID_TAG_PAGINA_CMS));
                pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.IDPestanyaCMSProyecto]);
            }

            if (!pListaTagsFiltros.ContainsKey((short)TiposTags.CategoriaTesauro))
            {
                pListaTagsFiltros.Add((short)TiposTags.CategoriaTesauro, new List<string>());
            }

            pListaTagsFiltros[(short)TiposTags.CategoriaTesauro].AddRange(BuscarTagFiltroEnCadena(ref pTags, Constantes.CAT_PROY));
            pListaTodosTags.AddRange(pListaTagsFiltros[(short)TiposTags.CategoriaTesauro]);
        }

        #endregion

        #region Actualización de la BD



        /// <summary>
        /// Descarga del servidor el mapeo del tesauro solicitado
        /// </summary>
        ///<param name="pProyectoID">Identificador del proyecto</param>
        ///<param name="pNombreDocumento">Nombre del documento de mapeo</param>
        ///<param name="pFicheroConfiguracionBD">Fichero de configuracion de BD</param>
        ///<param name="pUrlServicioArchivos">Cadena con la url del servicio de archivos</param>
        /// <returns>Contenido del fichero de mapeo</returns>
        public byte[] ObtenerMapeoTesauro(Guid pProyectoID, string pNombreDocumento, string pFicheroConfiguracionBD, EntityContext entityContext, LoggingService loggingService, RedisCacheWrapper redisCacheWrapper, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            DocumentacionCL docCL = null;

            if (!string.IsNullOrEmpty(pFicheroConfiguracionBD))
            {
                docCL = new DocumentacionCL(pFicheroConfiguracionBD, pFicheroConfiguracionBD, entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
            }
            else
            {
                docCL = new DocumentacionCL(entityContext, loggingService, redisCacheWrapper, mConfigService, servicesUtilVirtuosoAndReplication);
            }

            byte[] arrayMapping = docCL.ObtenerDocumentoMapeoTesauro(pProyectoID, pNombreDocumento);

            if (arrayMapping == null)
            {
                string urlServicioArchivos = mConfigService.ObtenerUrlServicio("urlArchivos");
                if (urlServicioArchivos.StartsWith("https://"))
                {
                    urlServicioArchivos = urlServicioArchivos.Replace("https://", "http://");
                }
                CallTokenService callTokenService = new CallTokenService(mConfigService);
                TokenBearer token = callTokenService.CallTokenApi();
                string result = CallWebMethods.CallGetApiToken(urlServicioArchivos, $"ObtenerMappingTesauro?pNombreMapeo={pNombreDocumento}", token);
                docCL.GuardarDocumentoMapeoTesauro(pProyectoID, arrayMapping, pNombreDocumento);
            }

            docCL.Dispose();

            return arrayMapping;
        }

        #endregion

        #endregion

        /// <summary>
        /// Envía un mensaje a la cuenta de errores y guarda un log del error
        /// </summary>
        protected void EnviarCorreoErroresUltimas24Horas(string pCorreoDestinatario, EntityContext entityContext, LoggingService loggingService, EntityContextBASE entityContextBASE, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {
            string cuerpo = "";

            //Recursos de comunidad
            BaseComunidadCN brRecComCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, -1, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
            int numBrRecComCN = brRecComCN.ObtenerNumeroElementosEnXHoras(24, EstadosColaTags.Reintento1, EstadosColaTags.Reintento1);
            brRecComCN.Dispose();
            if (numBrRecComCN > 0)
            {
                cuerpo += "Hay " + numBrRecComCN + " filas con estado fallido en la tabla ColaTagsComunidades en las últimas 24 horas.\n";
            }

            //Personas y organizaciones
            BaseComunidadCN brPerOrgCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, -1, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
            int numBrPerOrgCN = brPerOrgCN.ObtenerNumeroElementosEnXHoras(24, EstadosColaTags.Reintento1, EstadosColaTags.Reintento1);
            brPerOrgCN.Dispose();
            if (numBrPerOrgCN > 0)
            {
                cuerpo += "Hay " + numBrPerOrgCN + " filas con estado fallido en la tabla ColaTagsCom_Per_Org en las últimas 24 horas.\n";
            }

            //Comunidades de MyGnoss
            BaseComunidadCN brProyectosCN = new BaseComunidadCN(mFicheroConfiguracionBDBase, entityContext, loggingService, entityContextBASE, mConfigService, servicesUtilVirtuosoAndReplication);
            int numBrProyectosCN = brProyectosCN.ObtenerNumeroElementosEnXHoras(24, EstadosColaTags.Reintento1, EstadosColaTags.Reintento1);
            brProyectosCN.Dispose();
            if (numBrProyectosCN > 0)
            {
                cuerpo += "Hay " + numBrProyectosCN + " filas con estado fallido en la tabla ColaTagsProyectos en las últimas 24 horas.";
            }

            if (!string.IsNullOrEmpty(cuerpo))
            {
                List<ParametroAplicacion> filasConfiguracion = GestorParametroAplicacionDS.ParametroAplicacion;
                Dictionary<string, string> parametros = new Dictionary<string, string>();
                foreach (ParametroAplicacion fila in filasConfiguracion)
                {
                    parametros.Add(fila.Parametro, fila.Valor);
                }

                if (parametros.Count > 0)
                {
                    //UtilCorreo.EnviarCorreo((string)parametros["ServidorSmtp"], int.Parse(parametros["PuertoSmtp"]), (string)parametros["UsuarioSmtp"], (string)parametros["PasswordSmtp"], pCorreoDestinatario, (string)parametros["CorreoErrores"], (string)parametros["CorreoErrores"], (string)parametros["CorreoErrores"], (string)parametros["CorreoErrores"], asunto, cuerpo, false, Guid.Empty);
                    EnviarCorreoErrorYGuardarLog(cuerpo, "Correo estado modulo base, 24 horas", entityContext, loggingService);
                }
            }
        }

        #endregion

        #endregion

        #region Propiedades

        public Dictionary<Guid, string> DicUrlMappingProyecto
        {
            get
            {
                if (mDicUrlMappingProyecto == null)
                {
                    mDicUrlMappingProyecto = new Dictionary<Guid, string>();
                }

                return mDicUrlMappingProyecto;
            }
            set
            {
                mDicUrlMappingProyecto = value;
            }
        }

        /// <summary>
        /// Ruta relativa donde se encuentra el fichero de mapeo del Tesauro Semántico
        /// </summary>
        public string UrlMappingCategorias(EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {

            string salida = string.Empty;

            if (DicUrlMappingProyecto.ContainsKey(FilaProyecto.ProyectoID))
            {
                salida = DicUrlMappingProyecto[FilaProyecto.ProyectoID];
            }
            else
            {
                string mapping = FilaParametroGeneral(entityContext, loggingService, servicesUtilVirtuosoAndReplication).UrlMappingCategorias;

                //compruebo que UrlMappingCategorias no es nulo para agregar el directorio, ya que más tarde se comprueba si UrlMappingCategorias es vacio
                if (!string.IsNullOrEmpty(mapping))
                {
                    //antes se leía desde /config, ahora lo carga el servicio de archivos -> pasar el nombre de archivo
                    //salida = Path.GetDirectoryName(Assembly.GetExecutingAssembly().Location) + Path.DirectorySeparatorChar + mapping;

                    //UrlMapping será, entre [] lo opcional,  [NombreDocumentoMapeoSemantico.xml]|[NombreDocumentoMapeoComunidad.xml]
                    //de no haber ningún documento tampoco habrá |
                    if (mapping.Contains("|"))
                    {
                        //si sólo existe mapeo de comunidad, el nombre empezará en tubería
                        if (mapping.StartsWith("|"))
                        {
                            salida = mapping.Replace("|", string.Empty);
                        }
                        else
                        {
                            salida = mapping.Substring(mapping.IndexOf("|") + 1);
                        }

                        DicUrlMappingProyecto.Add(FilaProyecto.ProyectoID, salida);
                    }
                }
            }

            return salida;

        }

        public ParametroGeneral FilaParametroGeneral(EntityContext entityContext, LoggingService loggingService, IServicesUtilVirtuosoAndReplication servicesUtilVirtuosoAndReplication)
        {

            if (mFilaParametroGeneral == null)
            {
                ParametroGeneralCN paramGralCN = new ParametroGeneralCN(mFicheroConfiguracionBD, entityContext, loggingService, mConfigService, servicesUtilVirtuosoAndReplication);
                ParametroGeneral filaParamGral = paramGralCN.ObtenerFilaParametrosGeneralesDeProyecto(FilaProyecto.ProyectoID);
                paramGralCN.Dispose();
                mFilaParametroGeneral = filaParamGral;
            }

            return mFilaParametroGeneral;

        }

        public Proyecto FilaProyecto { get; set; }

        public Dictionary<Guid, Dictionary<string, List<string>>> DicPropiedadesOntologia
        {
            get
            {
                if (mDicPropiedadesOntologia == null)
                {
                    mDicPropiedadesOntologia = new Dictionary<Guid, Dictionary<string, List<string>>>();
                }
                return mDicPropiedadesOntologia;
            }

            protected set { mDicPropiedadesOntologia = new Dictionary<Guid, Dictionary<string, List<string>>>(); }
        }

        #endregion

        #region Métodos sobreescritos

        protected override ControladorServicioGnoss ClonarControlador()
        {
            Controlador controlador = new Controlador(mReplicacion, mRutaBaseTriplesDescarga, mUrlTriplesDescarga, mEmailErrores, mHoraEnvioErrores, mEscribirFicheroExternoTriples, ScopedFactory, mConfigService);
            return controlador;
        }

        #endregion

    }
}
