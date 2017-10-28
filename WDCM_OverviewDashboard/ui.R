### ---------------------------------------------------------------------------
### --- WDCM Dashboard Module, v. Beta 0.1
### --- Script: ui.R, v. Beta 0.1
### ---------------------------------------------------------------------------

### --- Setup
rm(list = ls())
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
                     h2('WDCM Overview Dashboard'),
                     HTML('<font size="3"><b>Wikidata Concepts Monitor</b></font>')
                     )
            ), # - fluidRow Title END
            
            # - fluidRow Logo
            fluidRow(
              column(width = 12,
                     img(src='Wikidata-logo-en.png', 
                         align = "left")
              )
            ), # - fluidRow END
            
            # - hr()
            fluidRow(
              column(width = 12,
                     hr()
              )
            ),
            
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
                            
                            # - tabPanel Description
                            tabPanel("Description",
                                     fluidRow(
                                       column(width = 8,
                                              HTML('<h2>WDCM Overview Dashboard</h2>
<h4>Description<h4>
                                                   <hr>
                                                   <h4>Introduction<h4>
                                                   <br>
                                                   <p><font size = 2>This Dashboard is a part of the <b>Wikidata Concepts Monitor (WDMC)</b>. The WDCM system provides analytics on Wikidata usage
                                                   across the client projects. The WDCM Overview Dashboard presents the big picture of Wikidata usage; other WDCM dashboards go
                                                   into more detail. The Overview Dashboard provides insights into <b>(1)</b> the similarities between the client projects in respect to their use of 
                                                   of Wikidata, as well as <b>(2)</b> the volume of Wikidata usage in every client project, <b>(3)</b> Wikidata usage tendencies, described by the volume of 
                                                   Wikidata usage in each of the semantic categories of items that are encompassed by the current WDCM edition, <b>(4)</b> the similarities between the 
                                                   Wikidata semantic categories of items in respect to their usage across the client projects, <b>(5)</b> ranking of client projects in respect to their 
                                                   Wikidata usage volume, <b>(6)</b> the Wikidata usage breakdown across the types of client projects and Wikidata semantic categories.</font></p>
                                                   <hr>
                                                   <h4>Definitions</h4>
                                                   <br>
                                                   <p><font size = 2><b>N.B.</b> The current <b>Wikidata item usage statistic</b> definition is <i>the count of the number of pages in a particular client project
                                                   where the respective Wikidata item is used</i>. Thus, the current definition ignores the usage aspects completely. This definition is motivated by the currently 
                                                   present constraints in Wikidata usage tracking across the client projects 
                                                   (see <a href = "https://www.mediawiki.org/wiki/Wikibase/Schema/wbc_entity_usage" target = "_blank">Wikibase/Schema/wbc entity usage</a>). 
                                                   With more mature Wikidata usage tracking systems, the definition will become a subject 
                                                   of change. The term <b>Wikidata usage volume</b> is reserved for total Wikidata usage (i.e. the sum of usage statistics) in a particular 
                                                   client project, group of client projects, or semantic categories. By a <b>Wikidata semantic category</b> we mean a selection of Wikidata items that is 
                                                   that is operationally defined by a respective SPARQL query returning a selection of items that intuitivelly match a human, natural semantic category. 
                                                   The structure of Wikidata does not necessarily match any intuitive human semantics. In WDCM, an effort is made to select the semantic categories so to match 
                                                   the intuitive, everyday semantics as much as possible, in order to assist anyone involved in analytical work with this system. However, the choice of semantic 
                                                   categories in WDCM is not necessarily exhaustive (i.e. they do not necessarily cover all Wikidata items), neither the categories are necessarily 
                                                   mutually exclusive. The Wikidata ontology is very complex and a product of work of many people, so there is an optimization price to be paid in every attempt to 
                                                   adapt or simplify its present structure to the needs of a statistical analytical system such as WDCM. The current set of WDCM semantic categories is thus not 
                                                   normative in any sense and can become a subject of change in any moment, depending upon the analytical needs of the community.</font></p>
                                                   <p><font size = 2>The currently used <b>WDCM Taxonomy</b> of Wikidata items encompasses the following 14 semantic categories: <i>Geographical Object</i>, <i>Organization</i>, <i>Architectural Structure</i>, 
                                                   <i>Human</i>, <i>Wikimedia</i>, <i>Work of Art</i>, <i>Book</i>, <i>Gene</i>, <i>Scientific Article</i>, <i>Chemical Entities</i>, <i>Astronomical Object</i>, <i>Thoroughfare</i>, <i>Event</i>, 
                                                   and <i>Taxon</i>.</font></p>
                                                   <hr>
                                                   <h4>Wikidata Usage Overview</h4>
                                                   <br>
                                                   <p><font size = 2>The similarity structure in Wikidata usage <i>across the client projects</i> is presented. Each bubble represents a client project.
                                                   The size of the bubble reflects the volume of Wikidata usage in the respective project. Projects similar in respect to the semantics of Wikidata
                                                   usage are grouped together.<br>
                                                   The bubble chart is produced by performing a <a href="https://en.wikipedia.org/wiki/T-distributed_stochastic_neighbor_embedding" target="_blank">t-SNE dimensionality reduction</a> 
                                                   of the client project pairwise Euclidean distances derived from the Projects x Categories contingency table. Given that the original higher-dimensional space 
                                                   from which the 2D map is derived is rather constrained by the choice of a small number of semantic categories, the similarity mapping is somewhat 
                                                   imprecise and should be taken as an attempt at an approximate big picture of the client projects similarity structure only. More precise 2D maps of 
                                                   the similarity structures in client projects are found on the <a href = "http://wdcm.wmflabs.org/WDCM_SemanticsDashboard/" target = "_blank">WDCM Semantics Dashboard</a>, where each semantic category first receives an 
                                                   <a href = "https://en.wikipedia.org/wiki/Topic_model" target = "_blank">LDA Topic Model</a>, 
                                                   and the similarity structure between the client projects is then derived from project topical distributions.<br>
                                                   While the <i>Explore</i> tab presents a dynamic <a href = "http://hafen.github.io/rbokeh/" target="_blank">{Rbokeh}</a> visualization alongside 
                                                   the tools to explore it in detail, the <i>Highlights</i> tab shows a static <a href = "http://ggplot2.org/" target="_blank">{ggplot2}</a> plot with the most important client projects 
                                                   marked (<b>NOTE.</b> Only top five projects (of each project type) in respect to Wikidata usage volume are labeled).</font></p>
                                                   <hr>
                                                   <h4>Wikidata Usage Tendency</h4>
                                                   <br>
                                                   <p><font size = 2>The similarity structure in Wikidata usage <i>across the semantic categories</i> is presented. Each bubble represents a Wikidata semantic
                                                   category. The size of the bubble reflects the volume of Wikidata usage from the respective category. If two categories are found in proximity,
                                                   that means that the projects that tend to use the one also tend to use the another, and vice versa. Similarly to the Usage Overview, the 2D mapping is obtained by performing 
                                                   a <a href="https://en.wikipedia.org/wiki/T-distributed_stochastic_neighbor_embedding" target="_blank">t-SNE dimensionality reduction</a> 
                                                   of the pairwise category Euclidean distances derived from the Projects x Categories contingency table. </font></p>
                                                   <hr>
                                                   <h4>Wikidata Usage Distribution</h4>
                                                   <br>
                                                   <p><font size = 2>The plots are helpful to build an understanding of the relative range of Wikidata usage across the client projects.
                                                   In the <i>Project Usage Rank-Frequency</i> plot, each point represents a client project; Wikidata usage is represented on the vertical and
                                                   the project usage rank on the horizontal axis, while only top project (per project type) are labeled. The highly-skewed, asymmetrical
                                                   distribution reveals that a small fraction of client projects only accounts for a huge proportion of Wikidata usage.<br> In the
                                                   <i>Project Usage log(Rank)-log(Frequency)</i> plot, the logarithms of both variables are represented. 
                                                   A <a href = "https://en.wikipedia.org/wiki/Power_law" target="_blank">power-law</a> relationship holds true if this
                                                   plot is linear. The plot includes the best linear fit, however, no attempts to estimate the underlying probability distribution were made. </font></p>
                                                   <hr>
                                                   <h4>Client Project Types</h4>
                                                   <br>
                                                   <p><font size = 2>Project types are provided in the rows of this chart, while the semantic categories are given on the horizontal axis.
                                                   The height of the respective bar indicates Wikidata usage volume from the respective semantic category in a particular client project.</font></p>
                                                   <hr>
                                                   <h4>Client Projects Usage Volume</h4>
                                                   <br>
                                                   <p><font size = 2>Use the slider to select the percentile rank range of the Wikidata usage volume distribution across the client project to show. The
                                                   chart will automatically adjust to present the selected projects in increasing order of Wikidata usage, and presenting at most 30 top projects
                                                   from the selection. <b>NOTE.</b> The <a href="https://en.wikipedia.org/wiki/Percentile_rank" target="_blank">percentile rank</a> 
                                                   of a score is the percentage of scores in its frequency distribution that are equal to or lower than it. 
                                                   For example, a client project that has a Wikidata usage volume greater than or equal to 75% of all client projects under
                                                   consideration is said to be at the 75th percentile, where 75 is the percentile rank.<br> In effect, you can browse the whole 
                                                   distribution of Wikidata usage across the client projects by selecting the lower and uppers limit in terms of usage percentile rank.</font></p>
                                                   <hr>
                                                   <h4>Wikidata Usage Browser</h4>
                                                   <br>
                                                   <p><font size = 2>A breakdown of Wikidata usage statistics across client projects and semantic categories. To the left, 
                                                   a table that presents a Client Project vs. Semantic Category cross-tabulation. The Usage column in this table is the Wikidata 
                                                  usage statistic for a particular Semantic Category x Client Project combination (e.g. The Wikidata usage in the category "Human" in 
                                                  the dewiki project). To the right, the total Wikidata usage per client project is presented (i.e. the sum of Wikidata usage across 
                                                  all semantic categories for a particular client project; e.g. the total Wikidata usage volume of enwiki).</font></p>
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
                                                   <li><b><a href = "http://wdcm.wmflabs.org/">WDCM Portal</a></b>.<br>
                                                   <font size = "2">The entry point to WDCM Dashboards.</font></li><br>
                                                   <li><b><a href = "http://wdcm.wmflabs.org/WDCM_OverviewDashboard/">WDCM Overview</a> (current dashboard)</b><br>
                                                   <font size = "2">The big picture. Fundamental insights in how Wikidata is used across the client projects.</font></li><br>
                                                   <li><b><a href = "http://wdcm.wmflabs.org/WDCM_SemanticsDashboard/">WDCM Semantics</a></b><br>
                                                   <font size = "2">Detailed insights into the WDCM Taxonomy (a selection of semantic categories from Wikidata), its distributional
                                                   semantics, and the way it is used across the client projects. If you are looking for Topic Models - that&#8217;s where
                                                   they live.</font></li><br>
                                                   <li><b><a href = "http://wdcm.wmflabs.org/WDCM_UsageDashboard/">WDCM Usage</a></b><br>
                                                   <font size = "2">Fine-grained information on Wikidata usage across client projects and project types. Cross-tabulations and similar..</font></li><br>
                                                   <li><b>WDCM Items</b><br>
                                                   <font size = "2">Fine-grained information on particular Wikidata item usage across the client projects.<b> (Under development)</b></font></li><br>
                                                   <li><b><a href = "https://wikitech.wikimedia.org/wiki/Wikidata_Concepts_Monitor">WDCM System Technical Documentation</a></b><br>
                                                   <font size = "2">The WDCM Wikitech Page.</font></li>
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
                     HTML('<b>Wikidata Concepts Monitor :: WMDE 2017</b><br>Diffusion: <a href="https://phabricator.wikimedia.org/diffusion/AWCM/" target = "_blank">WDCM</a><br>'),
                     HTML('Contact: Goran S. Milovanovic, Data Scientist, WMDE<br>e-mail: goran.milovanovic_ext@wikimedia.de
                          <br>IRC: goransm'),
                     br(),
                     br()
              )
            ) # - fluidRow Footer END
            
            ) # - fluidPage END
  
) # - ShinyUI END
