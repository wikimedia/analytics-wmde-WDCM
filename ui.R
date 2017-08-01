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

# - options
options(warn = -1)

### --- User Interface w. {shinydashboard}

shinyUI(
  
  ### --- dashboardPage
  ### --------------------------------
  
  dashboardPage(skin = "black",
                
                ### --- dashboarHeader
                ### --------------------------------
                
                dashboardHeader(
                  # - Title
                  title = "WDCM"
                  ), 
                ### ---- END dashboardHeader
                
                ### --- dashboardSidebar
                ### --------------------------------
                
                dashboardSidebar(
                  sidebarMenu(
                    id = "tabsWDCM",
                    menuItem(text = "Semantic Model", 
                             tabName = "semmodeltab", 
                             icon = icon("barcode"),
                             selected = TRUE
                    ),
                    menuItem(text = "Client Projects", 
                             tabName = "clientprojects", 
                             icon = icon("bar-chart", class = NULL, lib = "font-awesome")
                    )
                  )
                  
                ),
                ### --- END dashboardSidebar
                
                ### --- dashboardBody
                ### --------------------------------
                
                dashboardBody(
                  tabItems(
                    
                    ### --- TAB: Overview
                    ### --------------------------------
                    
                    tabItem(tabName = "semmodeltab",
                            fluidRow(
                              column(width = 12,
                                     shinydashboard::box(width = 12,
                                         solidHeader = T,
                                         status = "primary",
                                         title = "Semantic Model",
                                         fluidRow(
                                           column(width = 12,
                                                  HTML('This tab provides an overview of the <i>Wikidata Concepts Monitor</i> 
                                                       (<i>WDCM</i>) current <i>Semantic Model</i> and the elementary <i>Wikidata usage statistics</i>. The Semantic Model is obtained 
                                                       from Latent Dirichlet Allocation (LDA) over a <i>Projects</i> x <i>Items</i> counts matrix. 
                                                      The model is used to make assessments of the similarity between the Wikidata 
                                                       usage patterns in different client projects.<br>'), 
                                                  HTML('If you wish to assess Wikidata usage statistics for various client projects, 
                                                       the '),
                                                  a("Client Projects", href="#shiny-tab-projects", "data-toggle" = "tab"),
                                                  HTML(' tab is probably what you are looking for.'), HTML('The overview on this tab presents global <a href="#wsustats"<i>Wikidata usage statistics</i></a> and 
                                                       the <a href="#semodel"<i>Semantic Topics</i></a> derived from the Wikidata usage patterns.'), 
                                                  HTML('<p style="color:red">Please be patient until the plots load upon any action. This Dashboard utilizes interactive graphics heavily.</p>')
                                                  )
                                           )
                                         )
                                     )
                              ),
                            fluidRow(
                              column(width = 12,
                                       shinydashboard::box(width = 12,
                                           fluidRow(
                                             column(width = 12,
                                                    HTML("<h4 id='wsustats'>Wikidata Usage Statistics</h4>"),
                                                    hr()
                                                    )
                                           ),
                                           fluidRow(
                                             column(width = 4,
                                                    withSpinner(valueBoxOutput("numItemsBox", width = 12), size = .5)
                                                    ),
                                             column(width = 4,
                                                    withSpinner(valueBoxOutput("numProjectsBox", width = 12), size = .5)
                                                    ),
                                             column(width = 4,
                                                    withSpinner(valueBoxOutput("numCategoriesBox", width = 12), size = .5)
                                                    )
                                             )
                                           )
                                     )
                            ),
                            fluidRow(
                              column(width = 6,
                                     tabBox(width = 12,
                                            title = "Frequently used Wikidata items",
                                            id = "mostuseditems",
                                            selected = "Items Cloud",
                                            tabPanel(title = "Items Cloud",
                                                     id = "itemcloud",
                                                     fluidRow(
                                                       column(width = 12,
                                                              withSpinner(plotOutput('itemFrequencyWordCloud')),
                                                              hr()
                                                       )
                                                     ),
                                                     fluidRow(
                                                       column(width = 12,
                                                              HTML('Item size corresponds to how often is the respective item used across client projects.')
                                                              )
                                                     )
                                            ),
                                            tabPanel(title = "Items Barplot",
                                                     id = "itembars",
                                                     fluidRow(
                                                       column(width = 12,
                                                              withSpinner(plotOutput('itemFrequencyBarPlot')),
                                                              hr()
                                                       )
                                                     ),
                                                     fluidRow(
                                                       column(width = 12,
                                                              HTML('The barplot presents the 30 most frequently used Wikidata items.')
                                                              )
                                                       )
                                                     )
                                            )
                              ),
                              column(width = 6,
                                     tabBox(width = 12,
                                            title = "Wikidata volume in projects",
                                            id = "projectsvolume",
                                            selected = "Project Volume Cloud",
                                            tabPanel(title = "Project Volume Cloud",
                                                     id = "projectscloud",
                                                     fluidRow(
                                                       column(width = 12,
                                                              withSpinner(plotOutput('projectVolumeWordCloud')),
                                                              hr()
                                                       )
                                                     ),
                                                     fluidRow(
                                                       column(width = 12,
                                                              HTML('Project size corresponds to how much does the respective project use Wikidata.')
                                                       )
                                                     )
                                            ),
                                            tabPanel(title = "Project Volume Barplot",
                                                     id = "projectvolumebars",
                                                     fluidRow(
                                                       column(width = 12,
                                                              withSpinner(plotOutput('projectVolumeBarPlot')),
                                                              hr()
                                                       )
                                                     ),
                                                     fluidRow(
                                                       column(width = 12,
                                                              HTML('The barplot presents the top 20 client projects according to the usage of Wikidata.')
                                                       )
                                                     )
                                            )
                                     )
                                     )
                              ),
                            fluidRow(
                              column(width = 6,
                                     tabBox(width = 12,
                                            title = "Item category usage",
                                            id = "categoryusage",
                                            selected = "Category Volume Cloud",
                                            tabPanel(title = "Category Volume Cloud",
                                                     id = "categoriescloud",
                                                     fluidRow(
                                                       column(width = 12,
                                                              withSpinner(plotOutput('categoryVolumeWordCloud')),
                                                              hr()
                                                       )
                                                     ),
                                                     fluidRow(
                                                       column(width = 12,
                                                              HTML('Category size corresponds to how often is the category of items used across client projects.')
                                                       )
                                                     )
                                            ),
                                            tabPanel(title = "Category Volume Barplot",
                                                     id = "categoryvolumebars",
                                                     fluidRow(
                                                       column(width = 12,
                                                              withSpinner(plotOutput('categoryVolumeBarPlot')),
                                                              hr()
                                                       )
                                                     ),
                                                     fluidRow(
                                                       column(width = 12,
                                                              HTML('Categories in this barplot are ordered according to their total usage.')
                                                       )
                                                     )
                                            )
                                     )
                              ),
                              column(width = 6,
                                     tabBox(width = 12,
                                            title = "Aspects usage",
                                            id = "aspectusage",
                                            selected = "Aspects Cloud",
                                            tabPanel(title = "Aspects Cloud",
                                                     id = "aspectcloud",
                                                     fluidRow(
                                                       column(width = 12,
                                                              withSpinner(plotOutput('aspectVolumeWordCloud')),
                                                              hr()
                                                       )
                                                     ),
                                                     fluidRow(
                                                       column(width = 12,
                                                              HTML('Aspect size corresponds to how much it used across the client projects.')
                                                       )
                                                     )
                                            ),
                                            tabPanel(title = "Aspects Barplot",
                                                     id = "aspectbars",
                                                     fluidRow(
                                                       column(width = 12,
                                                              withSpinner(plotOutput('aspectsBarPlot')),
                                                              hr()
                                                       )
                                                     ),
                                                     fluidRow(
                                                       column(width = 12,
                                                              HTML('The barplot presents the top 30 aspects according to their usage across the client projects.')
                                                       )
                                                     )
                                            )
                                     )
                              )
                            ),
                            fluidRow(
                              column(width = 12,
                                     shinydashboard::box(width = 12,
                                         HTML("<h4 id='semodel'>Semantic Model</h4>"),
                                         hr(),
                                         fluidRow(
                                           column(width = 3,
                                                  selectizeInput("selectCategory",
                                                                 "Select Item Category:",
                                                                 multiple = F,
                                                                 choices = NULL,
                                                                 selected = NULL)
                                                  
                                           ),
                                           column(width = 3,
                                                  uiOutput("selectCatTopic")
                                                  )
                                         ),
                                         fluidRow(
                                           column(width = 6,
                                                  tabBox(width = 12,
                                                         title = "Topic Items Overview",
                                                         id = "topicoverview",
                                                         selected = "Topic Items Cloud",
                                                         tabPanel(title = "Topic Items Cloud",
                                                                  id = "topicsitemcloud",
                                                                  fluidRow(
                                                                    column(width = 12,
                                                                           withSpinner(plotOutput('catTopicItemWordCloud')),
                                                                           hr()
                                                                    )
                                                                  ),
                                                                  fluidRow(
                                                                    column(width = 12,
                                                                           HTML('Item size corresponds to the item probability in the selected topic.')
                                                                    )
                                                                  )
                                                         ),
                                                         tabPanel(title = "Topic Items Barplot",
                                                                  id = "topicbars",
                                                                  fluidRow(
                                                                    column(width = 12,
                                                                           withSpinner(plotOutput('topicItemsBarPlot')),
                                                                           hr()
                                                                    )
                                                                  ),
                                                                  fluidRow(
                                                                    column(width = 12,
                                                                           HTML('The barplot presents the top 30 most important items in this topic.')
                                                                    )
                                                                  )
                                                         )
                                                  )
                                           ),
                                           column(width = 6,
                                                  tabBox(width = 12,
                                                         title = "Topic Projects Overview",
                                                         id = "topicprojectoverview",
                                                         selected = "Topic Projects Cloud",
                                                         tabPanel(title = "Topic Projects Cloud",
                                                                  id = "topicsprojectsloud",
                                                                  fluidRow(
                                                                    column(width = 12,
                                                                           withSpinner(plotOutput('catTopicProjectsCloud')),
                                                                           hr()
                                                                    )
                                                                  ),
                                                                  fluidRow(
                                                                    column(width = 12,
                                                                           HTML('Project size corresponds to the probability of the selected topic in the respective project.')
                                                                    )
                                                                  )
                                                         ),
                                                         tabPanel(title = "Topic Projects Barplot",
                                                                  id = "topicprojectsbars",
                                                                  fluidRow(
                                                                    column(width = 12,
                                                                           withSpinner(plotOutput('catTopicProjectsBarPlot')),
                                                                           hr()
                                                                    )
                                                                  ),
                                                                  fluidRow(
                                                                    column(width = 12,
                                                                           HTML('The barplot presents the top 20 most important client projects in this topic.')
                                                                    )
                                                                  )
                                                         )
                                                  )
                                           )
                                         ),
                                         fluidRow(
                                           column(width = 12,
                                             tabBox(
                                               width = 12,
                                               title = "Topic Semantic Structure",
                                               id = "itemSemanticStructures",
                                               selected = "Topic Network",
                                               tabPanel(title = "Topic Network",
                                                        id = "topicsPanel1",
                                                        fluidRow(
                                                          column(width = 12,
                                                                 h3("Semantic Network: Items in Topic"),
                                                                 withSpinner(visNetwork::visNetworkOutput('topicStructure', height = 800)),
                                                                 hr(), HTML('The 100 most important items for this topic are shown; each node represents an Wikidata item. Each node points to its most proximal
                                                               neighbor in terms of the similarity of their usage patterns. You can drag a particular node to observe its neighborhood better; use mouse wheel to zoom in our out, left-click on the node to
                                                               uncover its item label.')
                                                                 )
                                                          )
                                                        ),
                                               tabPanel(title = "Topic Map",
                                                        id = "topicsPanel2",
                                                        fluidRow(
                                                          column(width = 12,
                                                                 h3("Semantic Map: Items in Topic"),
                                                                 withSpinner(rbokeh::rbokehOutput('topicMap', width = 1200, height = 800)),
                                                                 hr(), HTML('The 100 most important items for this topic are shown; each point represents an Wikidata item. The proximity
                                                                          between any two points corresponds to the similairy in their usage patterns. You can use the
                                                                          Bokeh interactive controls to the right to inspect the plot in detail.')
                                                                 )
                                                          )
                                                        ),
                                               tabPanel(title = "Topic Hierarchy",
                                                        id = "topicsPanel3",
                                                        fluidRow(
                                                          column(width = 12,
                                                                 h3("Semantic Hierarchy: Items in Topic"),
                                                                 withSpinner(networkD3::radialNetworkOutput('topicHierarchy', height = 800)),
                                                                 hr(), HTML('The 100 most important items for this topic are shown; each end node represents a Wikidata item.
                                                                            The nodes are structured according to the similairty in  their usage patterns; similar items are found
                                                                            under same and connected branches of the radial tree.')
                                                                     )
                                                                 )
                                                          )
                                             )
                                           )
                                         ),
                                         fluidRow(
                                           column(width = 12,
                                                  tabBox(
                                                    width = 12,
                                                    title = "Projects Semantic Structure",
                                                    id = "projectSemanticStructures",
                                                    selected = "Projects Network",
                                                    tabPanel(title = "Projects Network",
                                                             id = "projectsPanel1",
                                                             fluidRow(
                                                               column(width = 12,
                                                                      h3("Semantic Network: Projects in Category"),
                                                                      withSpinner(visNetwork::visNetworkOutput('catProjectStructure', height = 800)),
                                                                      hr(), HTML('Each node represents a client project and points to its most proximal
                                                           neighbor in terms of their topical similarity in this category. You can drag a particular node to observe its neighborhood better; use mouse wheel to zoom in our out, left-click on the node to
                                                                                 uncover its dominant topic.')
                                                                      )
                                                                      )
                                                               ),
                                                    tabPanel(title = "Projects Map",
                                                             id = "projectsPanel2",
                                                             fluidRow(
                                                               column(width = 12,
                                                                      h3("Semantic Map: Projects in Category"),
                                                                      withSpinner(rbokeh::rbokehOutput('catProjectMap', width = 1200, height = 800)),
                                                                      hr(), HTML('Each client project is represented by one point in this semantic map. The proximity between any two points corresponds to the similairy the Wikidata usage patterns of the respective projects. You can use the
                                                                                 Bokeh interactive controls to the right to inspect the plot in detail.')
                                                                      )
                                                                      )
                                                               ),
                                                    tabPanel(title = "Projects Hierarchy",
                                                             id = "projectsPanel3",
                                                             fluidRow(
                                                               column(width = 12,
                                                                      h3("Semantic Hierarchy: Projects in Category"),
                                                                      withSpinner(networkD3::radialNetworkOutput('catProjectHierarchy', height = 800)),
                                                                      hr(), HTML('Each end node represents a client project.
                                                                            The nodes are structured according to the similairty in Wikidata usage patterns; similar projects are found
                                                                                 under same and connected branches of the radial tree.')
                                                                      )
                                                                      )
                                                               )
                                                             )
                                                    )
                                           )
                                         )
                                     )
                              )
                    ),
                    ### --- END TAB: Overview
                    
                    ### --- TAB: Projects
                    ### --------------------------------
                    
                    tabItem(tabName = "clientprojects",
                            fluidRow(
                              column(width = 12,
                                     shinydashboard::box(width = 12,
                                         solidHeader = T,
                                         status = "primary",
                                         title = "Selection",
                                         fluidRow(
                                           column(width = 12,
                                                  fluidRow(
                                                    column(width = 12,
                                                           HTML('<b>USAGE:</b> Leave blank to select <i>all within a choice field</i>; click <font color="red"><i>Apply Selection</i></font> to update the results.'),
                                                           br(),
                                                           HTML('<b>NOTE:</b> Entity Usage counts are dependent on the current selection.'),
                                                           hr()
                                                           )
                                                    ),
                                                  fluidRow(
                                                    column(width = 4,
                                                           selectizeInput('projects',
                                                                          'Projects',
                                                                          choices = NULL,
                                                                          selected = NULL,
                                                                          multiple = TRUE
                                                                          )
                                                           ),
                                                    column(width = 4,
                                                           selectizeInput('categories',
                                                                          'Categories',
                                                                          choices = NULL,
                                                                          selected = NULL,
                                                                          multiple = TRUE
                                                                          )
                                                           ),
                                                    column(width = 4,
                                                           selectizeInput('languages',
                                                                          'Labels',
                                                                          choices = NULL,
                                                                          selected = NULL,
                                                                          multiple = TRUE
                                                                          )
                                                           )
                                                  ),
                                                    fluidRow(
                                                      column(width = 10
                                                             ),
                                                      column(width = 2,
                                                             actionButton('applySelection',
                                                                          label = "Apply Selection",
                                                                          width = '100%',
                                                                          icon = icon("database", 
                                                                                      class = NULL, 
                                                                                      lib = "font-awesome")
                                                                          )
                                                             )
                                                      )
                                                    )
                                                  )
                                           )
                                         )
                                     ),
                            fluidRow(
                              column(width = 12,
                                     tabBox(width = 12,
                                            title = "Client Projects Statistics",
                                            id = 'clientstatistics',
                                            selected = 'Tabulations',
                                            tabPanel(
                                              title = "Tabulations",
                                              id = "tabulationsPanel",
                                              fluidRow(
                                                column(width = 6,
                                                           fluidRow(
                                                             column(width = 12,
                                                                    h4("Projects"),
                                                                    hr(),
                                                                    withSpinner(plotOutput('tabulations_projectsChart')),
                                                                    downloadButton('tabulations_projectsDownload_Frame',
                                                                                   ''),
                                                                    hr()
                                                                    )
                                                             )
                                                       ),
                                                column(width = 6,
                                                           fluidRow(
                                                             column(width = 12,
                                                                    h4("Categories"),
                                                                    hr(),
                                                                    withSpinner(plotOutput('tabulations_categoriesChart')),
                                                                    downloadButton('tabulations_categoriesDownload_Frame',
                                                                                   ''),
                                                                    hr()
                                                                    )
                                                             )
                                                       )
                                                ),
                                              fluidRow(
                                                column(width = 6,
                                                           fluidRow(
                                                             column(width = 12,
                                                                    h4("Aspects"),
                                                                    hr(),
                                                                    withSpinner(plotOutput('tabulations_aspectsChart')),
                                                                    downloadButton('tabulations_aspectsDownload_Frame',
                                                                                   ''),
                                                                    hr()
                                                                    )
                                                             )
                                                       ),
                                                column(width = 6,
                                                           fluidRow(
                                                             column(width = 12,
                                                                    h4("Labels"),
                                                                    hr(),
                                                                    withSpinner(plotOutput('tabulations_LAspectsChart')),
                                                                    downloadButton('tabulations_LAspectsDownload_Frame',
                                                                                   ''),
                                                                    hr()
                                                                    )
                                                             )
                                                       )
                                                )
                                              ),
                                            tabPanel(
                                              title = "Cross-Tabulations",
                                              id = "crosstabulationsPanel",
                                              fluidRow(
                                                column(width = 6,
                                                           fluidRow(
                                                             column(width = 12,
                                                                    h4("Projects/Categories"),
                                                                    hr(),
                                                                    fluidRow(
                                                                      column(width = 12,
                                                                             withSpinner(plotOutput("crosstabulations_ProjectCategory_Chart"))
                                                                             )
                                                                    ),
                                                                    fluidRow(
                                                                      column(width = 1,
                                                                             downloadButton('crosstabulations_ProjectCategoryDownload_Frame',
                                                                                   '')
                                                                             ),
                                                                      column(width = 3 ,
                                                                             checkboxInput('Log_crosstabulations_ProjectCategory',
                                                                                  "log(Count + 1)",
                                                                                  value = FALSE)
                                                                             )
                                                                      ),
                                                                    fluidRow(
                                                                      column(width = 12,
                                                                             hr())
                                                                    )
                                                                    )
                                                           )
                                                ),
                                                column(width = 6,
                                                           fluidRow(
                                                             column(width = 12,
                                                                    h4("Projects/Aspects"),
                                                                    hr(),
                                                                    fluidRow(
                                                                      column(width = 12,
                                                                             withSpinner(plotOutput("crosstabulations_ProjectAspect_Chart"))
                                                                      )
                                                                    ),
                                                                    fluidRow(
                                                                      column(width = 1,
                                                                             downloadButton('crosstabulations_ProjectAspectDownload_Frame',
                                                                                            '')
                                                                      ),
                                                                      column(width = 3 ,
                                                                             checkboxInput('logcrosstabulations_ProjectAspect',
                                                                                           "log(Count + 1)",
                                                                                           value = FALSE)
                                                                      )
                                                                    ),
                                                                    fluidRow(
                                                                      column(width = 12,
                                                                             hr())
                                                                    )
                                                             )
                                                           )
                                                )
                                              ),
                                              fluidRow(
                                                column(width = 12,
                                                       fluidRow(
                                                             column(width = 12,
                                                                    h4("Projects/Labels"),
                                                                    hr(),
                                                                    fluidRow(
                                                                      column(width = 12,
                                                                             withSpinner(plotOutput("crosstabulations_ProjectLAspects_Chart"))
                                                                      )
                                                                    ),
                                                                    fluidRow(
                                                                      column(width = 1,
                                                                             downloadButton('crosstabulations_ProjectLAspectDownload_Frame',
                                                                                            '')
                                                                      ),
                                                                      column(width = 3 ,
                                                                             checkboxInput('Log_crosstabulations_ProjectLAspect',
                                                                                           "log(Count + 1)",
                                                                                           value = FALSE)
                                                                      )
                                                                    ),
                                                                    fluidRow(
                                                                      column(width = 12,
                                                                             hr())
                                                                    )
                                                             )
                                                           )
                                                )
                                              ),
                                              fluidRow(
                                                column(width = 6,
                                                       fluidRow(
                                                             column(width = 12,
                                                                    h4("Categories/Aspects"),
                                                                    hr(),
                                                                    fluidRow(
                                                                      column(width = 12,
                                                                             withSpinner(plotOutput("crosstabulations_CategoryAspects_Chart"))
                                                                      )
                                                                    ),
                                                                    fluidRow(
                                                                      column(width = 1,
                                                                             downloadButton('crosstabulations_CategoryAspects_Download_Frame',
                                                                                            '')
                                                                      ),
                                                                      column(width = 3 ,
                                                                             checkboxInput('logcrosstabulations_CategoryAspects',
                                                                                           "log(Count + 1)",
                                                                                           value = FALSE)
                                                                      )
                                                                    ),
                                                                    fluidRow(
                                                                      column(width = 12,
                                                                             hr())
                                                                    )
                                                             )
                                                           )
                                                )
                                              ),
                                              fluidRow(
                                                column(width = 12,
                                                           fluidRow(
                                                             column(width = 12,
                                                                    h4("Categories/Labels"),
                                                                    hr(),
                                                                    fluidRow(
                                                                      column(width = 12,
                                                                             withSpinner(plotOutput("crosstabulations_CategoryLAspects_Chart"))
                                                                      )
                                                                    ),
                                                                    fluidRow(
                                                                      column(width = 1,
                                                                             downloadButton('crosstabulations_CategoryLAspects_Download_Frame',
                                                                                            '')
                                                                      ),
                                                                      column(width = 3 ,
                                                                             checkboxInput('logcrosstabulations_CategoryLAspects',
                                                                                           "log(Count + 1)",
                                                                                           value = FALSE)
                                                                      )
                                                                    ),
                                                                    fluidRow(
                                                                      column(width = 12,
                                                                             hr())
                                                                    )
                                                             )
                                                           )
                                                )
                                              )
                                              ),
                                            tabPanel(
                                              title = "Project Semantics",
                                              id = "projectsemantics",
                                              fluidRow(
                                                column(width = 12,
                                                       h4("Project Semantics"),
                                                       hr(),
                                                       fluidRow(
                                                         column(width = 12,
                                                                withSpinner(plotOutput("projectSemantics_Chart", 
                                                                                       width = "100%", height = "800px"))
                                                         )
                                                       ),
                                                       fluidRow(
                                                         column(width = 1,
                                                                downloadButton('projectSemantics_Chart_Download_Frame',
                                                                               '')
                                                                )
                                                         ),
                                                       fluidRow(
                                                         column(width = 12,
                                                                hr(),
                                                                HTML('The grid will present charts for up to first 20 selected projects; the full dataset for the current selection will be provided on download.<br>
                                                                     <b>NOTE:</b> Topics are relative to the category-specific semantic models, of course.'))
                                                         )
                                                       )
                                              )
                                              
                                            )
                                            )
                                     )
                            )
                    )
                    ### --- END TAB: Projects
                                        
      ) ### --- END tabItems
      
    ) ### --- END dashboardBody
    
  ) ### --- dashboardPage
  
) # END shinyUI

