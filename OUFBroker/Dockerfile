ARG VERSION=0.0.0.0

FROM mcr.microsoft.com/dotnet/aspnet:5.0-alpine AS base
WORKDIR /app

EXPOSE 8443
EXPOSE 61616
EXPOSE 61617

ENV DOTNET_CLI_TELEMETRY_OPTOUT=1

ENV ASPNETCORE_URLS=http://+:8443
ENV ROUTER_ENDPOINT=tcp://*:61616
ENV PUBLISHER_ENDPOINT=tcp://*:61617
ENV CORS_ALLOWED_ORIGINS=http://localhost:8080
ENV API_KEY=aaaa

FROM mcr.microsoft.com/dotnet/sdk:5.0-alpine AS build
WORKDIR /src
COPY ["nuget.config", "."]
COPY ["OUFBroker/OUFBroker.csproj", "OUFBroker/"]
COPY ["OUFLib/OUFLib.csproj", "OUFLib/"]
RUN dotnet restore "OUFBroker/OUFBroker.csproj"
COPY . .
WORKDIR "/src/OUFBroker"
RUN dotnet build "OUFBroker.csproj" -c Release -o /app/build

FROM build AS publish
ARG VERSION
RUN dotnet publish "OUFBroker.csproj" -c Release -o /app/publish -p:Version=$VERSION

FROM base AS final
WORKDIR /app
COPY --from=publish /app/publish .
ENTRYPOINT ["dotnet", "OUFBroker.dll"]