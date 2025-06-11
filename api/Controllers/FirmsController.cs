using Microsoft.AspNetCore.Mvc;
using api.Models.DTOs;
using api.Models.Entities;
using api.Services;
using System.Diagnostics;
using System.Text.Json;
using Microsoft.AspNetCore.Authorization;

namespace api.Controllers
{
    [ApiController]
    [Route("api/")]
    public class FirmsController : ControllerBase
    {

        private readonly DataBaseService _dataBaseService;
        public FirmsController(DataBaseService dataBaseService)
        {
            _dataBaseService = dataBaseService;
        }

        [HttpGet("{minutes}")]
        public async Task<List<DataDto>> GetLastData(int minutes)
        {
            return await _dataBaseService.GetLatestInMinAsync(minutes);
        }
    }
}
