SELECT *
FROM `covid19project-380802.covid19project.coviddeaths`
WHERE continent IS NOT NULL

SELECT *
FROM `covid19project-380802.covid19project.covidvaccinations`
WHERE continent IS NOT NULL

SELECT location,date,total_cases,total_deaths
FROM `covid19project-380802.covid19project.coviddeaths`
WHERE continent IS NOT NULL

SELECT location,date,total_cases,total_deaths,(total_deaths/total_cases)*100 AS deaths_percentage
FROM `covid19project-380802.covid19project.coviddeaths`
WHERE continent IS NOT NULL
ORDER BY 1,2

SELECT location,date,total_cases,total_deaths,(total_deaths/total_cases)*100 AS deaths_percentage
FROM `covid19project-380802.covid19project.coviddeaths`
WHERE location = 'Ukraine' AND continent IS NOT NULL

SELECT location,date,total_cases,total_deaths,(total_deaths/total_cases)*100 AS deaths_percentage
FROM `covid19project-380802.covid19project.coviddeaths`
WHERE continent IS NOT NULL and location like '%States%'

SELECT location,date,population,total_cases,(total_cases/population)*100 as infected_percentage
FROM `covid19project-380802.covid19project.coviddeaths`
WHERE continent IS NOT NULL and location like '%States%'

SELECT location,population,MAX(total_cases) AS highest_infected_count,MAX((total_cases/population))*100 AS highest_infected_percentage
FROM `covid19project-380802.covid19project.coviddeaths`
WHERE continent IS NOT NULL
GROUP BY location,population
ORDER BY highest_infected_percentage desc

SELECT location,MAX(total_deaths) AS total_death_count
FROM `covid19project-380802.covid19project.coviddeaths`
WHERE continent IS NOT NULL
GROUP BY location
ORDER BY total_death_count desc

SELECT location,MAX(total_deaths) AS total_death_count
FROM `covid19project-380802.covid19project.coviddeaths`
WHERE continent IS NULL
GROUP BY location
ORDER BY total_death_count desc

SELECT SUM(new_cases) as total_cases, SUM(cast(new_deaths as int)) as total_deaths, SUM(cast(new_deaths as int))/SUM(new_cases)*100 as deaths_percentage
From `covid19project-380802.covid19project.coviddeaths`
WHERE continent IS NOT NULL 

SELECT *
FROM `covid19project-380802.covid19project.coviddeaths` deat
JOIN `covid19project-380802.covid19project.covidvaccinations` vacc
  ON deat.location = vacc.location
  AND deat.date = vacc.date
WHERE deat.continent IS NOT NULL

SELECT deat.continent,deat.location,deat.date,deat.population,vacc.new_vaccinations
FROM `covid19project-380802.covid19project.coviddeaths` deat
JOIN `covid19project-380802.covid19project.covidvaccinations` vacc
  ON deat.location = vacc.location
  AND deat.date = vacc.date
WHERE deat.continent IS NOT NULL
ORDER BY 2,3

SELECT deat.continent,deat.location,deat.date,deat.population,vacc.new_vaccinations,SUM(CAST(vacc.new_vaccinations AS int64)) OVER (PARTITION BY deat.location ORDER BY deat.location, deat.date) AS rolling_count_vaccinated
FROM `covid19project-380802.covid19project.coviddeaths` deat
JOIN `covid19project-380802.covid19project.covidvaccinations` vacc
  ON deat.location = vacc.location
  AND deat.date = vacc.date
WHERE deat.continent IS NOT NULL
ORDER BY 2,3

WITH CTE_VacOverPop AS
(SELECT deat.continent,deat.location,deat.date,deat.population,vacc.new_vaccinations,SUM(CAST(vacc.new_vaccinations AS int64)) OVER (PARTITION BY deat.location ORDER BY deat.location, deat.date) AS rolling_count_vaccinated
FROM `covid19project-380802.covid19project.coviddeaths` deat
JOIN `covid19project-380802.covid19project.covidvaccinations` vacc
  ON deat.location = vacc.location
  AND deat.date = vacc.date
WHERE deat.continent IS NOT NULL
ORDER BY 2,3
)
SELECT *
FROM CTE_VacOverPop

WITH CTE_VacOverPop AS
(SELECT deat.continent,deat.location,deat.date,deat.population,vacc.new_vaccinations,SUM(CAST(vacc.new_vaccinations AS int64)) OVER (PARTITION BY deat.location ORDER BY deat.location, deat.date) AS rolling_count_vaccinated
FROM `covid19project-380802.covid19project.coviddeaths` deat
JOIN `covid19project-380802.covid19project.covidvaccinations` vacc
  ON deat.location = vacc.location
  AND deat.date = vacc.date
WHERE deat.continent IS NOT NULL
ORDER BY 2,3
)
SELECT *,(rolling_count_vaccinated/population)*100
FROM CTE_VacOverPop
