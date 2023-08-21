using Elasticsearch.API.Repositories;
using Microsoft.AspNetCore.Http;
using Microsoft.AspNetCore.Mvc;

namespace Elasticsearch.API.Controllers
{
    [Route("api/[controller]/[action]")]
    [ApiController]
    public class ECommerceController : ControllerBase
    {
        private readonly ECommerceRepository _repository;

        public ECommerceController(ECommerceRepository repository)
        {
            _repository = repository;
        }

        [HttpGet]
        public async Task<IActionResult> TermQuery(string customerFirstName)
        {
            return Ok(await _repository.TermQuery(customerFirstName));
        }

        [HttpPost]
        public async Task<IActionResult> TermsQuery(List<string> customerFirstName)
        {
            return Ok(await _repository.TermsQuery(customerFirstName));
        }
        [HttpGet]
        public async Task<IActionResult> PrefixQuery(string customerFullName)
        {
            return Ok(await _repository.PrefixQuery(customerFullName));
        }

        [HttpGet]
        public async Task<IActionResult> RangeQuery(double fromPrice, double toPrice)
        {
            return Ok(await _repository.RangeQuery(fromPrice, toPrice));
        }
    }
}
