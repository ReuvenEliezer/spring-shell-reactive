package com.nice.cli.commands;

import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import org.apache.commons.csv.CSVFormat;
import org.apache.commons.csv.CSVPrinter;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.http.HttpHeaders;
import org.springframework.http.HttpStatus;
import org.springframework.http.HttpStatusCode;
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.util.Assert;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.util.UriComponentsBuilder;
import org.w3c.dom.Document;
import org.w3c.dom.Node;
import org.w3c.dom.NodeList;
import org.xml.sax.SAXException;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import javax.xml.parsers.*;
import java.io.*;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;
import java.util.stream.Collectors;

import org.w3c.dom.Element;

import javax.xml.parsers.DocumentBuilder;
import javax.xml.parsers.DocumentBuilderFactory;

@ShellComponent
public class GithubCommand {

    private static final Logger logger = LoggerFactory.getLogger(GithubCommand.class);
    private static final int MAX_RETRY = 3;
    private static final Duration FIXED_DELAY_ON_RETRY = Duration.ofSeconds(1);
    private static final String ORGANIZATION = "nice-cxone";
    private static final String REPO_FIELD_NAME = "name";
    private static final String LANGUAGE_FIELD_NAME = "language";
    private static final String CONTENT_FIELD_NAME = "content";

    private final WebClient webClient;
    private final ObjectMapper objectMapper;


    @Autowired
    public GithubCommand(WebClient webClient, ObjectMapper objectMapper) {
        this.webClient = webClient;
        this.objectMapper = objectMapper;
    }


    /**
     * https://docs.github.com/en/rest/repos/repos?apiVersion=2022-11-28#list-organization-repositories
     *
     * @param token      https://docs.github.com/en/enterprise-cloud@latest/authentication/authenticating-with-saml-single-sign-on/authorizing-a-personal-access-token-for-use-with-saml-single-sign-on
     * @param artifactId
     */
    @ShellMethod(key = "aggregate-depended-repo", value = "Retrieve all repository names depend on a given dependency")
    void aggregateDependedRepo(@ShellOption(value = "-t") String token,
                               @ShellOption(value = "-r", help = "example: \"saas-platform-lib-java17\" (take it from url repo)") String repositoryName) throws IOException {
        Set<String> repositoriesNames = ConcurrentHashMap.newKeySet();
        AtomicInteger pageNum = new AtomicInteger(1); //TODO change to 1
        UriComponentsBuilder repoUriBuilder = buildRetrieveRepoUri();
        AtomicInteger javaRepoCounter = new AtomicInteger(0);
        HttpHeaders headers = buildHeader(token);
        Flux<List<String>> allJavaRepo = retrieveAllRepositoriesNames(repoUriBuilder, pageNum, headers, javaRepoCounter, Language.JAVA);
        PomData pomData = retrievePomData(repositoryName, headers).block(); //TODO do recursively for all sub-modules

        Flux<Map<String, String>> allContainsDependency = retrieveAllContainsDependency(pomData, allJavaRepo, headers, "pom.xml", repositoriesNames);

        //aggregate by module
        Map<String, Set<String>> moduleToReposAggregatedMap = aggregateByModule(allContainsDependency).block();
        Map<String, Set<String>> moduleToReposAggregatedSortedBySizeMap = sort(moduleToReposAggregatedMap);

        logger.info("total repositories: {}, repositoriesNames: {} out of {} java repositories", repositoriesNames.size(), repositoriesNames, javaRepoCounter.get());
        printToCsv(repositoryName, moduleToReposAggregatedSortedBySizeMap);
    }

    @ShellMethod(key = "analyze-repo")
    void analyzeRepo(@ShellOption(value = "-t") String token,
                     @ShellOption(value = "-r", help = "example: \"saas-platform-lib-java17\" (take it from url repo)") String repositoryName) throws IOException {
        HttpHeaders headers = buildHeader(token);
        UriComponentsBuilder contentsUri = buildRetrieveContents(repositoryName, "");
//        Map<String, Set<ModuleToDependOn>> artifactIdToDependOnMap = new ConcurrentHashMap<>();
//        String response = retrieveRepoContents(headers, contentsUri).block();
//        JsonNode root = readJsonNode(response);
//        processJsonNode(root, headers, repositoryName, pomData, artifactIdToDependOnAntByResultMap);
        List<PomModuleData> pomModuleData = extractSubModules(contentsUri, headers, repositoryName).collectList().block();
        Set<String> groupIds = pomModuleData.stream().map(PomModuleData::groupId).collect(Collectors.toSet());
        Assert.isTrue(groupIds.size() == 1, "groupIds.size() != 1");
        Set<String> artifactIds = pomModuleData.stream().map(PomModuleData::artifactIds).collect(Collectors.toSet());
        PomData pomData = new PomData(groupIds.iterator().next(), artifactIds);
        List<Map<String, Set<String>>> block = extractDependOnModules(contentsUri, headers, repositoryName, pomData).collectList().block();
        Map<String, Set<String>> artifactIdToDependOnMap = new HashMap<>();
        block.forEach(map -> map.forEach((key, value) -> artifactIdToDependOnMap.computeIfAbsent(key, k -> new HashSet<>()).addAll(value)));
        logger.info("artifactIdToDependOnMap: {}", artifactIdToDependOnMap);
        Map<String, Set<ModuleDependedData>> moduleToDependOnAndDependByDataMap = collectDependByModules(artifactIdToDependOnMap);
        printToCsv_(repositoryName + "_1", moduleToDependOnAndDependByDataMap);
    }

    @ShellMethod(key = "aggregate-depended-repo-final", value = "Retrieve all repository names depend on a given dependency")
    void aggregateDependedRepoFinal(@ShellOption(value = "-t") String token,
                                    @ShellOption(value = "-r", help = "example: \"saas-platform-lib-java17\" (take it from url repo)") String repositoryName) throws IOException {
        Set<String> repositoriesNames = ConcurrentHashMap.newKeySet();
        AtomicInteger pageNum = new AtomicInteger(1); //TODO change to 1
        UriComponentsBuilder repoUriBuilder = buildRetrieveRepoUri();
        AtomicInteger javaRepoCounter = new AtomicInteger(0);
        HttpHeaders headers = buildHeader(token);
        UriComponentsBuilder contentsUri = buildRetrieveContents(repositoryName, "");

        List<PomModuleData> block1 = extractSubModules(contentsUri, headers, repositoryName).collectList().block();
        Set<String> artifactIds = block1.stream().map(PomModuleData::artifactIds).collect(Collectors.toSet());
        Set<String> groupIds = block1.stream().map(PomModuleData::groupId).collect(Collectors.toSet());
        Assert.isTrue(groupIds.size() == 1, "groupIds.size() != 1");
        PomData pomData1 = new PomData(block1.get(0).groupId(), artifactIds);
        List<Map<String, Set<String>>> block = extractDependOnModules(contentsUri, headers, repositoryName, pomData1).collectList().block();
        Map<String, Set<String>> artifactIdToDependOnMap = new HashMap<>();
        block.forEach(map -> map.forEach((key, value) -> artifactIdToDependOnMap.computeIfAbsent(key, k -> new HashSet<>()).addAll(value)));
        logger.info("artifactIdToDependOnMap: {}", artifactIdToDependOnMap);

        Map<String, Set<ModuleDependedData>> addData = collectDependByModules(artifactIdToDependOnMap);

        Flux<List<String>> allJavaRepo = retrieveAllRepositoriesNames(repoUriBuilder, pageNum, headers, javaRepoCounter, Language.JAVA);

        Flux<Map<String, String>> allContainsDependency = retrieveAllContainsDependency(pomData1, allJavaRepo, headers, "pom.xml", repositoriesNames);

        //aggregate by module
        Map<String, Set<String>> moduleToReposAggregatedMap = aggregateByModule(allContainsDependency)
                .block();

        logger.info("total repositories: {}, repositoriesNames: {} out of {} java repositories", repositoriesNames.size(), repositoriesNames, javaRepoCounter.get());
        Map<String, CsvData> csvDataMap = mergeMaps(addData, moduleToReposAggregatedMap);
        printToCsv__(repositoryName + "__5-12-2023", csvDataMap);
    }

    private Map<String, CsvData> mergeMaps(Map<String, Set<ModuleDependedData>> addData,
                                           Map<String, Set<String>> moduleToReposAggregatedMap) {
        Map<String, CsvData> result = new HashMap<>();

        addData.forEach((module, resultWithDependBySet) -> {
            Set<String> repo = moduleToReposAggregatedMap.computeIfAbsent(module, k -> new HashSet<>());
            result.put(module, new CsvData(repo, resultWithDependBySet));
        });

        moduleToReposAggregatedMap.forEach((module, repoSet) ->
                result.computeIfAbsent(module, k -> new CsvData(repoSet != null ? repoSet : new HashSet<>(), new HashSet<>()))
        );

        return result;
    }

    private record CsvData(Set<String> repoNames, Set<ModuleDependedData> moduleToDependOnAndDependBy) {
    }

    private static Map<String, Set<ModuleDependedData>> collectDependByModules(Map<String, Set<String>> artifactIdToDependOnMap) {
        //collectDependBy
        Map<String, Set<String>> dependByToModuleMap = new HashMap<>();
        artifactIdToDependOnMap.forEach((moduleName, dependOn) ->
                dependOn.forEach(dependOnModule -> dependByToModuleMap.computeIfAbsent(dependOnModule, v -> new HashSet<>()).add(moduleName))
        );

        Map<String, Set<ModuleDependedData>> finalMap = new HashMap<>();
        for (Map.Entry<String, Set<String>> entry : artifactIdToDependOnMap.entrySet()) {
            String moduleName = entry.getKey();
            Set<String> moduleToDependOns = entry.getValue();
            Set<String> repoData = dependByToModuleMap.get(moduleName);
            Set<String> dependBy = Optional.ofNullable(repoData).orElse(new HashSet<>());
            finalMap.computeIfAbsent(moduleName, value -> new HashSet<>()).add(new ModuleDependedData(moduleToDependOns, dependBy));
        }
        return finalMap;
    }

    private record ModuleToDependOn(String subModuleName, Set<Dependency> dependOn) {
    }

    private record ModuleDependedData(Set<String> dependOn, Set<String> dependBy) {
    }

    private void processJsonNode(JsonNode node, HttpHeaders headers, String repositoryName,
                                 PomData pomData, Map<String, Set<ModuleToDependOn>> artifactIdToDependOnAntByResultMap) {
        for (JsonNode jsonNode : node) {
            String type = jsonNode.get("type").asText();
            String currentPath = jsonNode.get("path").asText();
            if (type.equals("dir") && !currentPath.endsWith("src")) {
                UriComponentsBuilder contentFileUriBuilder = buildRetrieveContents(repositoryName, currentPath);
                String content = retrieveRepoContents(headers, contentFileUriBuilder).block();
                JsonNode contentRoot = readJsonNode(content);

                processJsonNode(contentRoot, headers, repositoryName, pomData, artifactIdToDependOnAntByResultMap);
            } else if (type.equals("file") && currentPath.endsWith("pom.xml") && currentPath.contains("/")) { //ignore root project
                UriComponentsBuilder contentFileUriBuilder = buildRetrieveContents(repositoryName, currentPath);
                String contentFile = retrieveRepoContents(headers, contentFileUriBuilder).block();
                Document doc = extractXmlFile(contentFile);

                String parentArtifactId = extractParentArtifactId(doc);
                String artifactRootId = currentPath.substring(0, currentPath.indexOf("/"));
                Set<Dependency> dependencies = extractDependencies(doc).stream()
                        .filter(dependency -> (dependency.groupId.equals(pomData.groupId) || dependency.groupId.equals("${project.groupId}"))
                                && pomData.artifactIds.contains(artifactRootId))
//                                && pomData.artifactIds.stream().anyMatch(dependency.artifactId::contains))
                        .collect(Collectors.toSet());
//                if (!dependencies.isEmpty()) {
                String moduleName = extractModuleName(doc);
//                    logger.info("dependencies: (depend on) {}", dependencies);
                ModuleToDependOn moduleToDependOn = new ModuleToDependOn(moduleName, dependencies);
                logger.info("artifactRootId {}, result:{}", artifactRootId, moduleToDependOn);
                artifactIdToDependOnAntByResultMap.computeIfAbsent(artifactRootId, value -> new HashSet<>()).add(moduleToDependOn);
//                }
            }
        }
    }

    private static String extractModuleName(Document doc) {
        NodeList nodeList = doc.getElementsByTagName("name");
        if (nodeList != null && nodeList.getLength() > 0) {
            String artifactIdName = nodeList.item(0).getTextContent();
            if (!"${project.artifactId}".equals(artifactIdName)) {
                return artifactIdName; //take the next one, because first is parent artifactId
            }
        }
        return doc.getElementsByTagName("artifactId").item(1).getTextContent();
    }

    private Flux<Map<String, Set<String>>> extractDependOnModules(UriComponentsBuilder uri, HttpHeaders headers, String repositoryName, PomData pomData) {
        return retrieveRepoContents(headers, uri)
                .flatMapMany(response -> {
                    JsonNode root = readJsonNode(response);
                    return Flux.fromIterable(root)
                            .flatMap(jsonNode -> processNode(jsonNode, headers, repositoryName, pomData));
                });
    }

    private Flux<PomModuleData> extractSubModules(UriComponentsBuilder uri, HttpHeaders headers, String repositoryName) {
        return retrieveRepoContents(headers, uri)
                .flatMapMany(response -> {
                    JsonNode root = readJsonNode(response);
                    return Flux.fromIterable(root)
                            .flatMap(jsonNode -> extractSubModules(jsonNode, headers, repositoryName));
                });
    }


    private Flux<Map<String, Set<String>>> processNode(JsonNode jsonNode, HttpHeaders headers, String repositoryName, PomData pomData) {
        String type = jsonNode.get("type").asText();
        String currentPath = jsonNode.get("path").asText();

        if (type.equals("dir") && !currentPath.endsWith("src")) {
            UriComponentsBuilder uri = buildRetrieveContents(repositoryName, currentPath);
            return extractDependOnModules(uri, headers, repositoryName, pomData);
        } else if (type.equals("file") && currentPath.endsWith("pom.xml") && currentPath.contains("/")) {
            UriComponentsBuilder contentFileUriBuilder = buildRetrieveContents(repositoryName, currentPath);
            return retrieveRepoContents(headers, contentFileUriBuilder)
                    .flatMapMany(contentFile -> {
                        Document doc = extractXmlFile(contentFile);
                        String parentArtifactId = extractParentArtifactId(doc);
                        String artifactRootId = currentPath.substring(0, currentPath.indexOf("/"));
                        Set<String> dependencies = extractDependencies(doc).stream()
                                .filter(dependency -> (dependency.groupId.equals(pomData.groupId) || dependency.groupId.equals("${project.groupId}"))
                                        && pomData.artifactIds.contains(artifactRootId))
                                .map(Dependency::artifactId)
                                .collect(Collectors.toSet());

                        String moduleName = extractModuleName(doc);
                        return Mono.just(Map.of(moduleName, dependencies));
                    });
        }
        return Flux.empty();
    }

    private Flux<PomModuleData> extractSubModules(JsonNode jsonNode, HttpHeaders headers, String repositoryName) {
        String type = jsonNode.get("type").asText();
        String currentPath = jsonNode.get("path").asText();

        if (type.equals("dir") && !currentPath.endsWith("src")) {
            UriComponentsBuilder uri = buildRetrieveContents(repositoryName, currentPath);
            return extractSubModules(uri, headers, repositoryName);
        } else if (type.equals("file") && currentPath.endsWith("pom.xml")) {
            UriComponentsBuilder contentFileUriBuilder = buildRetrieveContents(repositoryName, currentPath);
            return retrieveRepoContents(headers, contentFileUriBuilder)
                    .flatMapMany(contentFile -> {
                        boolean isRoot = !currentPath.contains("/");
                        Document doc = extractXmlFile(contentFile);
                        String moduleName = extractModuleName(doc);
                        String groupId = extractGroupId(doc, isRoot);
                        return Mono.just(new PomModuleData(groupId, moduleName));
                    });
        }

        return Flux.empty();
    }

    private static HttpHeaders buildHeader(String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.ACCEPT, "application/vnd.github+json");
        headers.set(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", token));
        headers.set("X-GitHub-Api-Version", "2022-11-28");
        return headers;
    }

    private static UriComponentsBuilder buildRetrieveContents(String repoName, String path) {
        return UriComponentsBuilder.fromUriString(String.format("https://api.github.com/repos/%s/%s/contents/%s", ORGANIZATION, repoName, path));
    }

    /**
     * max repo per page is 100
     */
    private static UriComponentsBuilder buildRetrieveRepoUri() {
        return UriComponentsBuilder.fromUriString(String.format("https://api.github.com/orgs/%s/repos", ORGANIZATION)).queryParam("per_page", 100);
    }

    /**
     * https://api.github.com/repos/nice-cxone/saas-comment-data-model/contents/pom.xml
     * https://docs.github.com/en/free-pro-team@latest/rest/repos/contents?apiVersion=2022-11-28#get-repository-content
     *
     * @return
     */
    private Flux<Map<String, String>> retrieveAllContainsDependency(PomData pomData, Flux<List<String>> allJavaRepo,
                                                                    HttpHeaders headers, String path, Set<String> repositoriesNames) {
        return allJavaRepo.flatMapIterable(repoNames -> repoNames)
                .flatMap(repoName -> {
                    UriComponentsBuilder contentFileUriBuilder = buildRetrieveContents(repoName, path);
                    return retrieveDependency(pomData, headers, repositoriesNames, repoName, contentFileUriBuilder);
                });
    }

    private Mono<PomData> retrievePomData(String repositoryName, HttpHeaders headers) {
        UriComponentsBuilder uri = buildRetrieveContents(repositoryName, "pom.xml");
        return retrievePomData(headers, uri)
                .map(doc -> {
                    Set<String> modules = extractModules(doc);
                    String moduleName = extractModuleName(doc);
                    String groupId = extractGroupId(doc);
                    return new PomData(groupId, modules);
                });
    }


    private static String extractGroupId(Document doc) {
        return extractGroupId(doc, false);
    }

    private static String extractGroupId(Document doc, boolean isRoot) {
        NodeList groupIdNodeList = doc.getElementsByTagName("groupId");
        Node groupIdNode;
        if (isRoot) {
            groupIdNode = groupIdNodeList.item(1); //take the second one, because first is parent groupId
        } else {
            groupIdNode = groupIdNodeList.item(0); // take the first one (parent), because it is the only one
        }
        return groupIdNode.getTextContent();
    }

    private record PomModuleData(String groupId, String artifactIds) {
    }

    private record PomData(String groupId, Set<String> artifactIds) {
    }

    private Mono<Map<String, String>> retrieveDependency(PomData pomData, HttpHeaders headers, Set<String> repositoriesNames,
                                                         String repoName, UriComponentsBuilder contentFileUriBuilder) {
        return retrievePomData(headers, contentFileUriBuilder)
                .mapNotNull(pomXmlDoc -> {
                    Set<Dependency> dependencies = extractDependencies(pomXmlDoc);
                    String groupId = pomData.groupId;
                    Set<String> artifactIds = pomData.artifactIds;
                    Map<String, String> moduleToRepo = dependencies.stream()
                            .filter(dependency -> dependency.groupId.equals(groupId)
                                    && artifactIds.contains(dependency.artifactId))
                            .collect(Collectors.toMap(Dependency::artifactId, dep -> repoName));
                    if (moduleToRepo.isEmpty()) {
                        return null;
                    }
                    return moduleToRepo;
                })
                .doOnNext(data -> {
                    logger.info("repoName: {} depend on modules {}", repoName, data.keySet());
                    repositoriesNames.add(repoName);
                });
    }

    private Mono<Document> retrievePomData(HttpHeaders headers, UriComponentsBuilder contentFileUriBuilder) {
        return retrieveRepoContents(headers, contentFileUriBuilder)
                .filter(e -> !e.isBlank())
                .map(this::extractXmlFile);
    }

    private Mono<String> retrieveRepoContents(HttpHeaders headers, UriComponentsBuilder contentFileUriBuilder) {
        return webClient.get()
                .uri(contentFileUriBuilder.toUriString())
                .headers(httpHeaders -> httpHeaders.addAll(headers))
                .retrieve()
                .bodyToMono(String.class)
                .retryWhen(Retry.fixedDelay(MAX_RETRY, FIXED_DELAY_ON_RETRY))
                .onErrorResume(e -> {
                    printErrorMessage(e);
                    return Mono.empty();
                });
    }

    private Flux<List<String>> retrieveAllRepositoriesNames(UriComponentsBuilder uriBuilder, AtomicInteger page,
                                                            HttpHeaders headers, AtomicInteger javaRepoCounter, Language language) {
        return retrieveAllRepo(uriBuilder, page, headers, javaRepoCounter, language)
                .expand(result -> {
                    if (hasMorePages(result)) {
                        return retrieveAllRepo(uriBuilder, page, headers, javaRepoCounter, language);
                    } else {
                        return Mono.empty();
                    }
                });
    }

    private Mono<List<String>> retrieveAllRepo(UriComponentsBuilder uriBuilder, AtomicInteger page,
                                               HttpHeaders headers, AtomicInteger javaRepoCounter, Language language) {
        final int pageNum = page.getAndIncrement();
        UriComponentsBuilder uriComponentsBuilder = uriBuilder.replaceQueryParam("page", pageNum);
        return webClient.get()
                .uri(uriComponentsBuilder.toUriString())
                .headers(httpHeaders -> httpHeaders.addAll(headers))
                .retrieve()
                .bodyToMono(String.class)
                .map(json -> extractRepoUrl(json, javaRepoCounter, language, pageNum))
                .onErrorResume(e -> {
                    printErrorMessage(e);
                    return Mono.empty();
                })
                .doOnNext(e -> logger.info("found {} repositoriesNames on page {}", e.size(), page))
                .retryWhen(Retry.fixedDelay(MAX_RETRY, FIXED_DELAY_ON_RETRY));
    }

    private static boolean hasMorePages(List<String> result) {
        return !result.isEmpty();
    }

    private List<String> extractRepoUrl(String json, AtomicInteger javaRepoCounter, Language language, int pageNum) {
        JsonNode rootNode = readJsonNode(json);
        validateAllRepoResponse(json, rootNode);
        return extractRepo(javaRepoCounter, language, pageNum, rootNode);
    }

    private static List<String> extractRepo(AtomicInteger javaRepoCounter, Language language, int pageNum, JsonNode rootNode) {
        List<String> repositoriesNames = new ArrayList<>();
        for (JsonNode jsonNode : rootNode) {
            String languageStr = jsonNode.get(LANGUAGE_FIELD_NAME).asText();
            if (language.language.equals(languageStr)) {
                String repoName = jsonNode.get(REPO_FIELD_NAME).asText();
                javaRepoCounter.getAndIncrement();
                repositoriesNames.add(repoName);
            }
        }
        if (repositoriesNames.isEmpty()) {
            logger.info("no repositories matched to {} language on page {}", language.language, pageNum);
        } else {
            logger.info("repositoriesNames: {}", repositoriesNames);
        }
        return repositoriesNames;
    }

    private Document extractXmlFile(String content) {
        String contentStr = extractContent(content);
        byte[] decodedBytes = Base64.getDecoder().decode(contentStr);
        String pomXmlFileContent = new String(decodedBytes, StandardCharsets.UTF_8);
        logger.debug("pom.xml: {}", pomXmlFileContent);
        try {
            DocumentBuilder builder = DocumentBuilderFactory.newInstance().newDocumentBuilder();
            ByteArrayInputStream input = new ByteArrayInputStream(pomXmlFileContent.getBytes(StandardCharsets.UTF_8));
            return builder.parse(input);
        } catch (SAXException | IOException | ParserConfigurationException e) {
            String errorMessage = String.format("failed to parse xml: %s", pomXmlFileContent);
            logger.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    private String extractContent(String json) {
        JsonNode rootNode = readJsonNode(json);
        String content = rootNode.get(CONTENT_FIELD_NAME).asText();
        return content.replaceAll("\n", "");
    }

    private JsonNode readJsonNode(String json) {
        try {
            return objectMapper.readTree(json);
        } catch (JsonProcessingException e) {
            String errorMessage = String.format("failed to parse json: %s", json);
            logger.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    private static Set<Dependency> extractDependencies(Document doc) {
        Set<Dependency> dependencies = new HashSet<>();
        NodeList dependenciesNode = doc.getElementsByTagName("dependency");
        for (int i = 0; i < dependenciesNode.getLength(); i++) {
            Node dependencyNode = dependenciesNode.item(i);
            if (dependencyNode.getNodeType() == Node.ELEMENT_NODE) {
                Element dependencyElement = (Element) dependencyNode;
                String groupId = dependencyElement.getElementsByTagName("groupId").item(0).getTextContent();
                String artifactId = dependencyElement.getElementsByTagName("artifactId").item(0).getTextContent();
                dependencies.add(new Dependency(groupId, artifactId));
            }
        }
        return dependencies;
    }

    private record Dependency(String groupId, String artifactId) {
    }

    private static Set<String> extractModules(Document doc) {
        Set<String> modules = new HashSet<>();
        NodeList moduleNodes = doc.getElementsByTagName("module");
        for (int i = 0; i < moduleNodes.getLength(); i++) {
            Node moduleNode = moduleNodes.item(i);
            String moduleName = moduleNode.getTextContent();
            modules.add(moduleName);
        }
        return modules;
    }

    private static String extractParentArtifactId(Document doc) {
        Node parentNode = doc.getElementsByTagName("parent").item(0);
        String parentArtifactId = null;
        if (parentNode.getNodeType() == Node.ELEMENT_NODE) {
            Element parentElement = (Element) parentNode;
            String parentGroupId = parentElement.getElementsByTagName("groupId").item(0).getTextContent();
            parentArtifactId = parentElement.getElementsByTagName("artifactId").item(0).getTextContent();
            String parentVersion = parentElement.getElementsByTagName("version").item(0).getTextContent();
        }

        return parentArtifactId;

    }

    private static void validateAllRepoResponse(String json, JsonNode rootNode) {
        if (!rootNode.isArray()) {
            String errorMessage = String.format("not an array result json: %s", json);
            logger.error(errorMessage);
            throw new UnsupportedOperationException(errorMessage);
        }
    }

    private static void printErrorMessage(Throwable e) {
        if (e.getCause() instanceof WebClientResponseException webClientResponseException) {
            HttpStatusCode statusCode = webClientResponseException.getStatusCode();
            if (statusCode.value() == HttpStatus.NOT_FOUND.value()) {
                logger.info("ignore from repo that not including pom.xml {}", e.getMessage());
            } else if (statusCode.value() == HttpStatus.FORBIDDEN.value()
                    || statusCode.value() == HttpStatus.TOO_MANY_REQUESTS.value()) {
                logger.error("Error occurred: API rate limit exceeded for user ID  {}", e.getMessage(), e);
                /**
                 * {
                 *     "message": "API rate limit exceeded for user ID 144931323. If you reach out to GitHub Support for help, please include the request ID EF65:CBAE:132BD4B7:136A4626:6565C271.",
                 *     "documentation_url": "https://docs.github.com/rest/overview/rate-limits-for-the-rest-api"
                 * }
                 */
            } else {
                logger.error("Error occurred: {}", e.getMessage(), e);
            }
        } else {
            logger.error("Error occurred: {}", e.getMessage(), e);
        }
    }

    private static void printToCsv_(String fileName, Map<String, Set<ModuleDependedData>> map) throws IOException {
        final CSVFormat csvFormat = CSVFormat.Builder.create()
                .setHeader("No.", "module", "depend on", "depend by")
                .build();
        try (FileWriter fileWriter = new FileWriter(fileName + ".csv");
             CSVPrinter printer = new CSVPrinter(fileWriter, csvFormat)) {
            int currentIndex = 1;
            for (Map.Entry<String, Set<ModuleDependedData>> entry : map.entrySet()) {
                String module = entry.getKey();
                Set<ModuleDependedData> repoByModule = entry.getValue();
                for (ModuleDependedData moduleToDependOnAndDependBy : repoByModule) {
                    printer.printRecord(currentIndex++, module, moduleToDependOnAndDependBy.dependOn, moduleToDependOnAndDependBy.dependBy);
                }
            }
        }
    }

    private static void printToCsv(String fileName, Map<String, Set<String>> moduleToUsedMap) throws IOException {
        final CSVFormat csvFormat = CSVFormat.Builder.create()
                .setHeader("No.", "module", "#usage (repositories)", "usage list-repositories")
                .build();
        try (FileWriter fileWriter = new FileWriter(fileName + ".csv");
             CSVPrinter printer = new CSVPrinter(fileWriter, csvFormat)) {
            int currentIndex = 1;
            for (Map.Entry<String, Set<String>> entry : moduleToUsedMap.entrySet()) {
                String module = entry.getKey();
                Set<String> repoByModule = entry.getValue();
                printer.printRecord(currentIndex++, module, repoByModule.size(), repoByModule.toString());
            }
        }
    }

    private static void printToCsv__(String fileName, Map<String, CsvData> csvDataMap) throws IOException {
        final CSVFormat csvFormat = CSVFormat.Builder.create()
                .setHeader("No.", "module", "#usage (repositories)", "usage list-repositories", "depend on", "depend by")
                .build();
        try (FileWriter fileWriter = new FileWriter(fileName + ".csv");
             CSVPrinter printer = new CSVPrinter(fileWriter, csvFormat)) {
            int currentIndex = 1;
            for (Map.Entry<String, CsvData> entry : csvDataMap.entrySet()) {
                String module = entry.getKey();
                CsvData csvData = entry.getValue();
                Set<String> repo = csvData.repoNames;
                Set<ModuleDependedData> repoByModule = csvData.moduleToDependOnAndDependBy;
                for (ModuleDependedData moduleToDependOnAndDependBy : repoByModule) {
                    printer.printRecord(currentIndex++, module, repo.size(), repo.toString(), moduleToDependOnAndDependBy.dependOn, moduleToDependOnAndDependBy.dependBy);
                }
            }
        }

    }

    private static LinkedHashMap<String, Set<String>> sort(Map<String, Set<String>> moduleToReposAggregatedMap) {
        return moduleToReposAggregatedMap.entrySet().stream()
                .sorted(Map.Entry.<String, Set<String>>comparingByValue(Comparator.comparingInt(Set::size)).reversed())
                .collect(LinkedHashMap::new, (acc, entry) -> acc.put(entry.getKey(), entry.getValue()), Map::putAll);
    }

    private static Mono<Map<String, Set<String>>> aggregateByModule(Flux<Map<String, String>> allContainsDependency) {
        return allContainsDependency
                .flatMap(map -> Flux.fromIterable(map.entrySet()))
                .groupBy(Map.Entry::getKey)
                .flatMap(groupedFlux -> groupedFlux
                        .map(Map.Entry::getValue)
                        .collect(Collectors.toSet())
                        .map(set -> Map.of(groupedFlux.key(), set))
                )
                .reduce(new HashMap<>(), (acc, map) -> {
                    acc.putAll(map);
                    return acc;
                });
    }

    private enum Language {
        JAVA("Java");
        private final String language;

        Language(String language) {
            this.language = language;
        }

    }

}
