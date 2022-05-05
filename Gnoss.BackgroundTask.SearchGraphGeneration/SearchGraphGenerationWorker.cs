using Es.Riam.Gnoss.Servicios;
using Es.Riam.Gnoss.Util.Configuracion;
using GnossServicioModuloBASE;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using System;
using System.Collections.Generic;
using System.Linq;
using System.Threading;
using System.Threading.Tasks;

namespace Gnoss.BackgroundTask.SearchGraphGeneration
{
    public class SearchGraphGenerationWorker : Worker
    {
        private readonly ILogger<SearchGraphGenerationWorker> _logger;
        private readonly ConfigService _configService;

        public SearchGraphGenerationWorker(ILogger<SearchGraphGenerationWorker> logger, ConfigService configService, IServiceScopeFactory scopeFactory) : base(logger, scopeFactory)
        {
            _logger = logger;
            _configService = configService;
        }

        protected override List<ControladorServicioGnoss> ObtenerControladores()
        {
            ControladorServicioGnoss.INTERVALO_SEGUNDOS = _configService.ObtenerIntervalo();

            string rutaBaseTriples = _configService.ObtenerRutaBaseTriples();
            string urlTriples = _configService.ObtenerUrlTriples();
            string emailError = _configService.ObtenerEmailErrores();
            int horaError = _configService.ObtenerHoraEnvioErrores();
            bool replicacion = _configService.ObtenerReplicacionActivada();
            bool escribirFicheroExternoTriples = _configService.ObtenerEscribirFicheroExternoTriples();
            List<ControladorServicioGnoss> controladores = new List<ControladorServicioGnoss>();
            controladores.Add(new ControladorColaRecursos(replicacion, rutaBaseTriples, urlTriples, emailError, horaError, escribirFicheroExternoTriples, ScopedFactory, _configService,  1));
            
            controladores.Add(new ControladorColaPerOrg(replicacion, rutaBaseTriples, urlTriples, emailError, horaError, escribirFicheroExternoTriples, ScopedFactory, _configService, 2));

            controladores.Add(new ControladorColaComunidades(replicacion, rutaBaseTriples, urlTriples, emailError, horaError, escribirFicheroExternoTriples, ScopedFactory, _configService, 3));

            controladores.Add(new ControladorColaPaginasCMS(replicacion, rutaBaseTriples, urlTriples, emailError, horaError, escribirFicheroExternoTriples, ScopedFactory, _configService, 4));

            return controladores;
        }
    }
}
