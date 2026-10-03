FROM mcr.microsoft.com/dotnet/sdk:10.0 AS build
WORKDIR /src
COPY src/Worker/Worker.csproj src/Worker/
RUN dotnet restore src/Worker/Worker.csproj
COPY src/Worker/ src/Worker/
RUN dotnet publish src/Worker/Worker.csproj -c Release -o /app/publish --no-restore

FROM mcr.microsoft.com/dotnet/runtime:10.0 AS final
WORKDIR /app
COPY --from=build /app/publish .
USER $APP_UID
ENTRYPOINT ["dotnet", "Worker.dll"]
