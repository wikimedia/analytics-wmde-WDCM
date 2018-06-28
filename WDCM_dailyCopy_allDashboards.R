### ---------------------------------------------------------------------------
### --- WDCM Process Module, v. Beta 0.1
### --- Script: WDCM_dailyCopy_allDashboards, v. Beta 0.1
### ---------------------------------------------------------------------------
### --- DESCRIPTION:
### --- copy all WDCM dashboards from their dev to their deployment directories
### --- on wikidataconcepts.eqiad.wmflabs
### ---------------------------------------------------------------------------

### ---------------------------------------------------------------------------
### --- LICENSE:
### ---------------------------------------------------------------------------
### --- GPL v2
### --- This file is part of Wikidata Concepts Monitor (WDCM)
### ---
### --- WDCM is free software: you can redistribute it and/or modify
### --- it under the terms of the GNU General Public License as published by
### --- the Free Software Foundation, either version 2 of the License, or
### --- (at your option) any later version.
### ---
### --- WDCM is distributed in the hope that it will be useful,
### --- but WITHOUT ANY WARRANTY; without even the implied warranty of
### --- MERCHANTABILITY or FITNESS FOR A PARTICULAR PURPOSE.  See the
### --- GNU General Public License for more details.
### ---
### --- You should have received a copy of the GNU General Public License
### --- along with WDCM. If not, see <http://www.gnu.org/licenses/>.
### ---------------------------------------------------------------------------

# - Biases Dashboard
system('sudo cp -r /home/goransm/WMDE/WDCM/WDCM_BiasesDashboard/ui.R /srv/shiny-server/WDCM_BiasesDashboard/', 
       wait = T)
system('sudo cp -r /home/goransm/WMDE/WDCM/WDCM_BiasesDashboard/server.R /srv/shiny-server/WDCM_BiasesDashboard/', 
       wait = T)

# - Geo Dashboard
system('sudo cp -r /home/goransm/WMDE/WDCM/WDCM_GeoDashboard/ui.R /srv/shiny-server/WDCM_GeoDashboard/', 
       wait = T)
system('sudo cp -r /home/goransm/WMDE/WDCM/WDCM_GeoDashboard/server.R /srv/shiny-server/WDCM_GeoDashboard/', 
       wait = T)

# - Overview Dashboard
system('sudo cp -r /home/goransm/WMDE/WDCM/WDCM_OverviewDashboard/ui.R /srv/shiny-server/WDCM_OverviewDashboard/', 
       wait = T)
system('sudo cp -r /home/goransm/WMDE/WDCM/WDCM_OverviewDashboard/server.R /srv/shiny-server/WDCM_OverviewDashboard/', 
       wait = T)

# - Semantics Dashboard
system('sudo cp -r /home/goransm/WMDE/WDCM/WDCM_SemanticsDashboard/ui.R /srv/shiny-server/WDCM_SemanticsDashboard/', 
       wait = T)
system('sudo cp -r /home/goransm/WMDE/WDCM/WDCM_SemanticsDashboard/server.R /srv/shiny-server/WDCM_SemanticsDashboard/', 
       wait = T)

# - Usage Dashboard
system('sudo cp -r /home/goransm/WMDE/WDCM/WDCM_UsageDashboard/ui.R /srv/shiny-server/WDCM_UsageDashboard/', 
       wait = T)
system('sudo cp -r /home/goransm/WMDE/WDCM/WDCM_UsageDashboard/server.R /srv/shiny-server/WDCM_UsageDashboard/', 
       wait = T)

# - Structure Dashboard
system('sudo cp -r /home/goransm/WMDE/WDCM/WDCM_StructureDashboard/ui.R /srv/shiny-server/WDCM_StructureDashboard/', 
       wait = T)
system('sudo cp -r /home/goransm/WMDE/WDCM/WDCM_StructureDashboard/server.R /srv/shiny-server/WDCM_StructureDashboard/', 
       wait = T)
system('sudo cp -r /home/goransm/WMDE/WDCM/WDCM_StructureDashboard/global.R /srv/shiny-server/WDCM_StructureDashboard/', 
       wait = T)

### --------------------------------------------

# - Technical Wishes: Advanced Search Extension
system('sudo cp /home/goransm/WMDE/TechnicalWishes/AdvancedSearchExtension/ui.R /srv/shiny-server/TW_AdvancedSearchExtension', 
       wait = T)
system('sudo cp /home/goransm/WMDE/TechnicalWishes/AdvancedSearchExtension/server.R /srv/shiny-server/TW_AdvancedSearchExtension', 
       wait = T)


