using Microsoft.AspNetCore.Builder;
using Microsoft.AspNetCore.Hosting;
using Microsoft.Extensions.Configuration;
using Microsoft.Extensions.DependencyInjection;
using Microsoft.Extensions.Hosting;
using Microsoft.OpenApi.Models;
using OUFBroker.Authentication;
using OUFBroker.Helpers;
using Swashbuckle.AspNetCore.Filters;
using System;
using System.IO;
using System.Reflection;

namespace OUFBroker
{
    public class Startup
    {
        public Startup(IConfiguration configuration)
        {
            Configuration = configuration;
        }

        public IConfiguration Configuration { get; }

        // This method gets called by the runtime. Use this method to add services to the container.
        public void ConfigureServices(IServiceCollection services)
        {
            services.AddCors();
            services.AddRouting(options => options.LowercaseUrls = true);

            services.AddMvc().AddJsonOptions(options => 
                OUFLib.Serialization.SerializationOptions.SetOptions(options.JsonSerializerOptions)
            );

            services.AddAuthentication(options => options.AddScheme(ApiKeyAuthenticationOptions.DefaultScheme,
                schemeBuilder => schemeBuilder.HandlerType = typeof(ApiKeyAuthenticationHandler)));

            services.AddControllers();

            services.AddSwaggerGen(c =>
            {
                c.SwaggerDoc("v1", new OpenApiInfo { Title = nameof(OUFBroker) });

                var securityScheme = new OpenApiSecurityScheme
                {
                    Name = Constants.API_KEY_HEADER_NAME,
                    Description = "Please enter your **_API key_**",
                    In = ParameterLocation.Header,
                    Type = SecuritySchemeType.ApiKey,
                    Scheme = Constants.API_KEY_POLICY_NAME
                };

                c.AddSecurityDefinition(Constants.API_KEY_POLICY_NAME, securityScheme);
                c.OperationFilter<SecurityRequirementsOperationFilter>();

                // Set the comments path for the Swagger JSON and UI.
                var xmlFile = $"{Assembly.GetExecutingAssembly().GetName().Name}.xml";
                var xmlPath = Path.Combine(AppContext.BaseDirectory, xmlFile);
                c.IncludeXmlComments(xmlPath);
            });

            services.ConfigureServices(Configuration);
        }

        // This method gets called by the runtime. Use this method to configure the HTTP request pipeline.
        public void Configure(IApplicationBuilder app, IWebHostEnvironment env, IHostApplicationLifetime applicationLifetime)
        {
            if (env.IsDevelopment())
            {
                app.UseDeveloperExceptionPage();
            }

            app.UseSwagger();
            app.UseSwaggerUI(c =>
            {
                c.DefaultModelsExpandDepth(-1);
                c.SwaggerEndpoint("/swagger/v1/swagger.json", nameof(OUFBroker));
            });

            app.UseHttpsRedirection();

            app.UseRouting();

            string[] origins = Configuration?.GetSection("CORS_ALLOWED_ORIGINS")?.Get<string>()?.Split(',');

            if (null != origins && origins.Length != 0)
            {
                app.UseCors(builder => builder.WithOrigins(origins)
                    .AllowCredentials()
                    .AllowAnyMethod()
                    .AllowAnyHeader()
                );
            }

            app.UseAuthorization();

            app.UseEndpoints(endpoints =>
            {
                endpoints.MapControllers();
            });
        }
    }
}
