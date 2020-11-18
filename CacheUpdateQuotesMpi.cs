using Calendar.Service.Interfaces;
using Microsoft.Extensions.Hosting;
using Microsoft.Extensions.Logging;
using NHibernate.Linq;
using scb.Engine;
using StackExchange.Redis;
using System;
using System.Collections.Generic;
using System.Text.Json;
using System.Threading;
using System.Threading.Tasks;

namespace Broker.Worker
{
    public class CacheUpdateQuotesMpi : BackgroundService
    {
        private readonly IConnectionMultiplexer _cache;
        private readonly ILogger _logger;
        private readonly ICalendarServices _calendarServices;

        public CacheUpdateQuotesMpi(IConnectionMultiplexer cache, ILogger<CacheUpdateQuotesMpi> logger, ICalendarServices calendarServices)
        {
            _cache = cache;
            _logger = logger;
            _calendarServices = calendarServices;
        }

        protected override async Task ExecuteAsync(CancellationToken stoppingToken)
        {
            while (!stoppingToken.IsCancellationRequested)
            {
                try
                {
                    if (await _calendarServices.IsTechBreakNow())
                    {
                        await Task.Delay(1000, stoppingToken);
                        continue;
                    }

                    await LoadQuotesMpi();
                    await Task.Delay(1000, stoppingToken);
                }

                catch { }
            }
        }


        #region private methods

        private async Task LoadQuotesMpi()
        {
            try
            {
                using var session = ServiceMan.ServiceManX.GetSession(ConnectionDic.Quik);
                var quotes = await session.Query<Services.CacheUpdateQuotesMpi>().ToListAsync();

                _logger.LogInformation($"Получено {quotes.Count} котировок из базы QUIK");

                var tasks = new List<Task>();

                foreach (var quote in quotes)
                {
                    tasks.Add(_cache.SetStringAsync(EnumsHelper.GetRedisKeyPrefixQuotesMpiSecCodeAndClassCode(quote.SecurCode, quote.ClassCode), JsonSerializer.Serialize(quote)));
                };

                await Task.WhenAll(tasks);

                _logger.LogInformation($"Выгружено в кэш {quotes.Count} котировок");
            }

            catch (Exception e)
            {
                _logger.LogError(e, $"{e.InnerException?.Message ?? e.Message}");
            }
        }

        #endregion
    }
}
