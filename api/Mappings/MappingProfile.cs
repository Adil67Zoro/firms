using api.Models.DTOs;
using api.Models.Entities;
using AutoMapper;
using MongoDB.Bson.Serialization.Attributes;

namespace api.Mappings
{
    public class MappingProfile : Profile
    {
        public MappingProfile()
        {

            CreateMap<Data, DataDto>();
        }
    }
}
