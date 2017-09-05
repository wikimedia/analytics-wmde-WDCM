
### ---------------------------------------------------------------------------
### --- WDCM Dashboard Module, v. Beta 0.1
### --- Script: ui.R, v. Beta 0.1
### ---------------------------------------------------------------------------

### --- Setup
rm(list=ls())
### --- general
library(shiny)
library(shinydashboard)
library(shinycssloaders)
### --- outputs
library(visNetwork)
library(rbokeh)
library(networkD3)
library(ggvis)
library(DT)

# - options
options(warn = -1)

shinyUI(
  
  fluidPage(title = 'WDCM Projects', 
            theme = NULL,
            
            # - fluidRow Title
            fluidRow(
              column(width = 12,
                     h2('WDCM Overview Dahsboard'),
                     HTML('<font size="3"><b>Wikidata Concepts Monitor</b></font>'),
                     hr()
                     )
            ), # - fluidRow Title END
            
            fluidRow(
              column(width = 12,
                     tabBox(id = 'MainBox', 
                            selected = 'Overview', 
                            title = '', 
                            width = 12,
                            height = NULL, 
                            side = 'left',
                            
                            # - tabPanel Overview
                            tabPanel("Overview", 
                                     fluidRow(
                                       column(width = 12,
                                              hr(),
                                              tabBox(width = 12,
                                                     title = "Wikidata Usage Overview",
                                                     id = "projectoverview",
                                                     selected = "Explore",
                                                     tabPanel(title = "Explore",
                                                             id = "projexplore",
                                                             fluidRow(
                                                               column(width = 12,
                                                                      h3('Explore Wikidata Usage'),
                                                                      HTML('Each bubble represents a client project. 
                                                                            The size of the bubble reflects the volume of Wikidata usage in the respective project; a logarithmic scale is used in this plot.<br> 
                                                                            Projects similar in respect to the semantics of Wikidata usage are grouped together. 
                                                                            Use the tools next to the plot legend to explore the plot and hover over bubbles for details.'),
                                                                      hr(),
                                                                      withSpinner(rbokeh::rbokehOutput('overviewPlotDynamic',
                                                                                             width = "1400px",
                                                                                             height = "900px")
                                                                                  )
                                                                      )
                                                               )
                                                             ),
                                                     tabPanel(title = "Highlights",
                                                              id = "projoverview",
                                                              fluidRow(
                                                                column(width = 12,
                                                                       h3('Wikidata Usage Highlights'),
                                                                       HTML('Each bubble represents a client project. 
                                                                            The size of the bubble reflects the volume of Wikidata usage in the respective project.<br> 
                                                                            Projects similar in respect to the semantics of Wikidata usage are grouped together. 
                                                                            Only top five projects (of each project type) in respect to Wikidata usage volume are labeled.'),
                                                                       hr(),
                                                                       withSpinner(plotOutput('overviewPlot',
                                                                                              width = "1400px",
                                                                                              height = "900px")
                                                                                   )
                                                                       )
                                                                )
                                                              )
                                                     )
                                              )
                                     ),
                                     hr(),
                                     fluidRow(
                                       column(width = 6,
                                              h4('Wikidata Usage Tendency'),
                                              HTML('<font size="2">Each bubble represents a Wikidata semantic category. These categories represent one possible way of categorizing the Wikidata items.
                                                   The size of the bubble reflects the volume of Wikidata usage from the respective category. 
                                                   If two categories are found in proximity, that means that the projects that tend to use the one 
                                                   also tend to use the another, and vice versa.</font>'),
                                              hr(),
                                              withSpinner(plotOutput('usageTendencyPlot',
                                                                     width = "900px",
                                                                     height = "700px")
                                              )
                                              ),
                                       column(width = 6,
                                              tabBox(width = 12,
                                                    title = "Wikidata Usage Distribution",
                                                    id = "usagedist",
                                                    selected = "Project Usage Rank-Frequency",
                                                    tabPanel(title = "Project Usage Rank-Frequency",
                                                             id = "RF",
                                                             fluidRow(
                                                               column(width = 12,
                                                                      br(),
                                                                      HTML('<font size="2">Each point represents a client project. 
                                                                           Wikidata usage is represented on the vertical and the project 
                                                                           usage rank on the horizontal axis. Only top projects per project type 
                                                                           are labeled.</font>'),
                                                                      hr(),
                                                                      withSpinner(plotOutput('projectRankFrequencyPlot',
                                                                                             width = "900px",
                                                                                             height = "700px")
                                                                                  )
                                                                      )
                                                               )
                                                             ),
                                                    tabPanel(title = "Project Usage log(Rank)-log(Frequency)",
                                                             id = "lRlF",
                                                             fluidRow(
                                                               column(width = 12,
                                                                      br(),
                                                                      HTML('<font size="2">Each point represents a client project. 
                                                                           The logarithms of Wikidata usage and project 
                                                                           usage rank are represented on on the vertical and horizontal axis, respectively. 
                                                                           Top three projects per project type are labeled.</font>'),
                                                                      hr(),
                                                                      withSpinner(plotOutput('projectLogRankLogFrequencyPlot', 
                                                                                             width = "900px",
                                                                                             height = "700px")
                                                                                  )
                                                                      )
                                                               )
                                                             )
                                                    )
                                              )
                                     ),
                                     hr(),
                                     fluidRow(
                                       column(width = 6,
                                              h4('Client Project Types'),
                                              HTML('<font size="2">Wikidata usage breakdown across the client project types. Each row represents one client project type. 
                                                   Semantic categories of Wikidata items are placed on the horizontal axis, while the respective usage counts are given on the vertical axis.</font>'),
                                              hr(),
                                              withSpinner(plotOutput('projectCategoryCross',
                                                                     width = "900px",
                                                                     height = "700px")
                                              )
                                              ),
                                       column(width = 6,
                                              h4('Client Projects Usage Volume'),
                                              HTML('<font size="2">Wikidata usage across the client projects. 
                                                    Use slider (below the chart) to select the range of client projects by percentile ranks*. 
                                                    <br><b>Note:</b> The chart present at most 30 top projects (in terms of Wikidata usage volume) from the selection.</font>'),
                                              hr(),
                                              withSpinner(plotOutput('projectVolume',
                                                                     width = "900px",
                                                                     height = "700px")
                                              ),
                                              sliderInput('volumeSlider', 
                                                          'Percentile Rank (select lower and upper limit):', 
                                                          min = 1, 
                                                          max = 100, 
                                                          value = c(95,100), 
                                                          step = 1, 
                                                          round = TRUE,
                                                          ticks = TRUE, 
                                                          animate = FALSE,
                                                          width = '100%'),
                                              HTML('<font size="2">*The <a href="https://en.wikipedia.org/wiki/Percentile_rank" target="_blank">percentile rank</a> 
                                                    of a score is the percentage of scores in its frequency distribution that are equal to or lower than it. 
                                                   For example, a client project that has a Wikidata usage volume greater than or equal to 75% of all client projects under
                                                   consideration is said to be at the 75th percentile, where 75 is the percentile rank.</font>')
                                              )
                                       ),
                                     hr(),
                                     fluidRow(
                                       column(width = 6,
                                              h3('Client Project + Semantic Category Usage Cross-Tabulation'),
                                              HTML(' Wikidata usage breakdown across the client projects, project types, and semantic categories. 
                                                   Sort the table by any of its columns or enter a search term to find a specific project, project type, or 
                                                   Wikidata semantic category.'),
                                              hr(),
                                              withSpinner(DT::dataTableOutput('projectCategoryDataTable', width = "100%"))
                                              ),
                                       column(width = 6,
                                              h3('Client Project Usage Tabulation'),
                                              HTML(' Wikidata usage per client project. 
                                                    Sort the table by any of its columns or enter a search term to find a specific project or project type.'),
                                              br(), br(),
                                              hr(),
                                              withSpinner(DT::dataTableOutput('projectDataTable', width = "100%"))
                                              )
                                       )
                                     ), # - tabPanel Overview END
                            
                            # - tabPanel Usage
                            tabPanel("Description",
                                     fluidRow(
                                       column(width = 12,
                                              HTML('<h2>WDCM Overview Dashboard</h2>
<h4>Description<h4>
                                                   <hr>
                                                   <h4>Introduction<h4>
                                                   <br>
                                                   <p><font size = 2>This Dashboard is a part of the Wikidata Concepts Monitor (WDMC). The WDCM system provides analytics on Wikidata usage
                                                   across the client projects. The WDCM Overview Dashboard presents the big picture of Wikidata usage; other WDCM dashboards go
                                                   into more detail.</font></p>
                                                   <hr>
                                                   <h4>Wikidata Item Usage Definition</h4>
                                                   <br>
                                                   <p><font size = 2><b>NOTE.</b> The current Wikidata item usage statistic definition is <i>the count of the number of pages in a particular client project
                                                   where the respective Wikidata item is used</i>. Thus, the current definition ignores the usage aspects completely</font></p>
                                                   <hr>
                                                   <h4>Wikidata Usage Overview</h4>
                                                   <br>
                                                   <p><font size = 2>The similarity structure in Wikidata usage <i>across the client projects</i> is presented. Each bubble represents a client project.
                                                   The size of the bubble reflects the volume of Wikidata usage in the respective project. Projects similar in respect to the semantics of Wikidata
                                                   usage are grouped together. Only top five projects (of each project type) in respect to Wikidata usage volume are labeled.<br>
                                                   The bubble chart is produced by performing a t-SNE dimensionality reduction of the inter-Projects Euclidean distances derived from the
                                                   Projects x Categories contingency table.</font></p>
                                                   <hr>
                                                   <h4>Wikidata Usage Tendency</h4>
                                                   <br>
                                                   <p><font size = 2>The similarity structure in Wikidata usage <i>across the semantic categories</i> is presented. Each bubble represents a Wikidata semantic
                                                   category. The size of the bubble reflects the volume of Wikidata usage from the respective category. If two categories are found in proximity,
                                                   that means that the projects that tend to use the one also tend to use the another, and vice versa.<br>
                                                   The bubble chart is produced by performing a t-SNE dimensionality reduction of the inter-Projects Euclidean distances derived from the
                                                   Categories x Projects contingency table.</font></p>
                                                   <hr>
                                                   <h4>Wikidata Usage Distribution</h4>
                                                   <br>
                                                   <p><font size = 2>The plots are helpful to build an understanding of the relative range of Wikidata usage across the client projects.
                                                   In the <i>Project Usage Rank-Frequency</i> plot, each point represents a client project; Wikidata usage is represented on the vertical and
                                                   the project usage rank on the horizontal axis, while only top project (per project type) are labeled. The highly-skewed, asymmetrical
                                                   distribution reveals that a small fraction of client projects only accounts for a huge proportion of Wikidata usage.<br> In the
                                                   <i>Project Usage log(Rank)-log(Frequency)</i> plot, the logarithms of both variables are represented. A power-law relationship holds true if this
                                                   plot is linear; the plot includes the best fit linear model, however, no attempts to estimate the Zipf distribution were made. </font></p>
                                                   <hr>
                                                   <h4>Client Project Types</h4>
                                                   <br>
                                                   <p><font size = 2>Project types are provided in the rows of this chart, while the semantic categories are given on the horizontal axis.
                                                   The height of the respective bar indicates Wikidata usage from the respective semantic category in a particular client project.</font></p>
                                                   <hr>
                                                   <h4>Client Projects Usage Volume</h4>
                                                   <br>
                                                   <p><font size = 2>Use the slider to select the percent range of the Wikidata usage distribution across the client project to show. The
                                                   chart will automatically adjust presenting the selected projects in increasing order of Wikidata usage, presenting at most 30 top projects
                                                   from the selection.</font></p>
                                                   <hr>
                                                   <h4>Wikidata Usage Browser</h4>
                                                   <br>
                                                   <p><font size = 2>A breakdown of Wikidata usage statistics across client projects, project types, and semantic categories.</font></p>
                                                   ')
                                       )
                                     )
                                     ), # - tabPanel Usage END
                            
                            # - tabPanel Navigate
                            tabPanel("Navigate WDCM", 
                                     fluidRow(
                                       column(width = 8,
                                              HTML('<h2>WDCM Navigate</h2>
                                                    <h4>Your orientation in the WDCM Dashboards System<h4>
                                                    <hr>
                                                    <ul>
                                                      <li><b>WDCM Overview</b> (current dashboard).<br>
                                                      <font size = "2">The big picture. Fundamental insights in how Wikidata is used across the client projects.</font></li><br>
                                                      <li><b>WDCM Semantics.</b><br>
                                                      <font size = "2">Detailed insights into the WDCM ontology (a selection of semantic categories from Wikidata), its distributional
                                                      semantics, and the way it is used across the client projects. If you are looking for Topic Models, yes that&#8217;s where
                                                      they live in WDCM.</font></li><br>
                                                      <li><b>WDCM Usage.</b><br>
                                                      <font size = "2">Fine-grained information on Wikidata usage across client projects and project types. Cross-tabulations and similar..</font></li><br>
                                                      <li><b>WDCM Items</b><br>
                                                      <font size = "2">Fine-grained information on particular Wikidata item usage across the client projects..</font></li><br>
                                                      <li><b>WDCM System Technical Documentation.</b><br>
                                                      <font size = "2">A document that will come to existence eventually. There are rumours of an existing draft.</font></li>
                                                    </ul>'
                                                   )
                                       )
                                     )
                                     ) # - tabPanel Structure END
                            
                            ) # - MainBox END
                     ) # - Main column End
              
            ), #- Main fluidRow END
            
            # - fluidRow Footer
            fluidRow(
              column(width = 12,
                     hr(),
                     HTML('<b>Wikidata Concepts Monitor :: WMDE 2017</b><br>GitHub: <a href="https://github.com/wmde/WDCM" target = "_blank">WDCM</a><br>'),
                     HTML('Contact: Goran S. Milovanovic, Data Analyst, WMDE<br>e-mail: goran.milovanovic_ext@wikimedia.de
                          <br>IRC: goransm'),
                     br(),
                     br()
              )
            ) # - fluidRow Footer END
            
            ) # - fluidPage END
  
) # - ShinyUI END
