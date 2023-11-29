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
import org.springframework.shell.standard.ShellComponent;
import org.springframework.shell.standard.ShellMethod;
import org.springframework.shell.standard.ShellOption;
import org.springframework.web.reactive.function.client.WebClient;
import org.springframework.web.reactive.function.client.WebClientResponseException;
import org.springframework.web.util.UriComponentsBuilder;
import reactor.core.publisher.Flux;
import reactor.core.publisher.Mono;
import reactor.util.retry.Retry;

import java.io.FileWriter;
import java.io.IOException;
import java.nio.charset.StandardCharsets;
import java.time.Duration;
import java.util.*;
import java.util.concurrent.ConcurrentHashMap;
import java.util.concurrent.atomic.AtomicInteger;

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
     * @param dependency
     */
    @ShellMethod(key = "retrieve-all-repository-names-contents-given-dependency", value = "Retrieve all repository names contents given dependency")
    public void retrieveRepoBasedOnGivenDependency(@ShellOption(value = "-t") String token, @ShellOption(defaultValue = "spring", value = "-s") String dependency) throws IOException {
        Set<String> repositoriesNames = ConcurrentHashMap.newKeySet();
        AtomicInteger pageNum = new AtomicInteger(1);
        UriComponentsBuilder repoUriBuilder = buildRetrieveRepoUri();
        HttpHeaders headers = buildHeader(token);
        AtomicInteger javaRepoCounter = new AtomicInteger(0);
        Flux<List<String>> allJavaRepo = retrieveAllRepositoriesNames(repoUriBuilder, pageNum, headers, javaRepoCounter, Language.JAVA);
        Flux<String> allContainsDependency = retrieveAllContainsDependency(dependency, allJavaRepo, headers, repositoriesNames);
        allContainsDependency.blockLast();
        logger.info("total repositories: {}, repositoriesNames: {} out of {} java repositories", repositoriesNames.size(), repositoriesNames, javaRepoCounter.get());
        printToCsv(new ArrayList<>(repositoriesNames));
    }

    /**
     * https://api.github.com/repos/nice-cxone/saas-comment-data-model/contents/pom.xml
     * https://docs.github.com/en/free-pro-team@latest/rest/repos/contents?apiVersion=2022-11-28#get-repository-content
     *
     * @return
     */
    private Flux<String> retrieveAllContainsDependency(String dependency, Flux<List<String>> allJavaRepo, HttpHeaders headers, Set<String> repositoriesNames) {
        return allJavaRepo.flatMapIterable(repoNames -> repoNames)
                .flatMap(repoName -> {
                    UriComponentsBuilder contentFileUriBuilder = buildRetrieveContentFileUri(repoName, "pom.xml");
                    return webClient.get()
                            .uri(contentFileUriBuilder.toUriString())
                            .headers(httpHeaders -> httpHeaders.addAll(headers))
                            .retrieve()
                            .bodyToMono(String.class)
                            .retryWhen(Retry.fixedDelay(MAX_RETRY, FIXED_DELAY_ON_RETRY))
                            .onErrorReturn("")
                            .filter(e -> !e.isBlank())
                            .filter(e -> {
                                String content = extractContent(e);
                                byte[] decodedBytes = Base64.getDecoder().decode(content);
                                String pomXmlFileContent = new String(decodedBytes, StandardCharsets.UTF_8);
//                                logger.info(pomXmlFileContent);
                                return pomXmlFileContent.contains(dependency);
                            })
                            .doOnNext(data -> {
                                logger.info("repoName: {}", repoName);
                                repositoriesNames.add(repoName);
                            })
                            .onErrorResume(e -> {
                                printErrorMessage(e);
                                return Mono.empty();
                            });
                });
    }

    private static void printToCsv(List<String> dependenciesResult) throws IOException {
        final CSVFormat csvFormat = CSVFormat.Builder.create()
                .setHeader("No.", "Repository Name")
                .build();
        try (FileWriter fileWriter = new FileWriter("dependenciesResult.csv");
             CSVPrinter printer = new CSVPrinter(fileWriter, csvFormat)) {
            for (int i = 0; i < dependenciesResult.size(); i++) {
                printer.printRecord(i + 1, dependenciesResult.get(i));
            }
        }
    }

    private static UriComponentsBuilder buildRetrieveContentFileUri(String repoName, String fileName) {
        return UriComponentsBuilder.fromUriString(String.format("https://api.github.com/repos/%s/%s/contents/%s", ORGANIZATION, repoName, fileName));
    }


    /**
     * max repo per page is 100
     */
    private static UriComponentsBuilder buildRetrieveRepoUri() {
        return UriComponentsBuilder.fromUriString(String.format("https://api.github.com/orgs/%s/repos", ORGANIZATION)).queryParam("per_page", 100);
    }

    private static HttpHeaders buildHeader(String token) {
        HttpHeaders headers = new HttpHeaders();
        headers.set(HttpHeaders.ACCEPT, "application/vnd.github+json");
        headers.set(HttpHeaders.AUTHORIZATION, String.format("Bearer %s", token));
        headers.set("X-GitHub-Api-Version", "2022-11-28");
        return headers;
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


    private boolean hasMorePages(List<String> result) {
        return !result.isEmpty();
    }

    private List<String> extractRepoUrl(String json, AtomicInteger javaRepoCounter, Language language, int pageNum) {
        try {
            JsonNode rootNode = objectMapper.readTree(json);
            validateAllRepoResponse(json, rootNode);
            return extractRepo(javaRepoCounter, language, pageNum, rootNode);
        } catch (JsonProcessingException e) {
            String errorMessage = String.format("failed to parse json: %s", json);
            logger.error(errorMessage, e);
            throw new RuntimeException(errorMessage, e);
        }
    }

    private static List<String> extractRepo(AtomicInteger javaRepoCounter, Language language, int pageNum, JsonNode rootNode) {
        List<String> repositoriesNames = new ArrayList<>();
        for (JsonNode jsonNode : rootNode) {
            String languageStr = jsonNode.get(LANGUAGE_FIELD_NAME).asText();
            if (language.getLanguage().equals(languageStr)) {
                String repoName = jsonNode.get(REPO_FIELD_NAME).asText();
                javaRepoCounter.getAndIncrement();
                repositoriesNames.add(repoName);
            }
        }
        if (repositoriesNames.isEmpty()) {
            logger.info("no repositories matched to {} language on page {}", language.getLanguage(), pageNum);
        } else {
            logger.info("repositoriesNames: {}", repositoriesNames);
        }
        return repositoriesNames;
    }

    private static void validateAllRepoResponse(String json, JsonNode rootNode) {
        if (!rootNode.isArray()) {
            String errorMessage = String.format("not an array result json: %s", json);
            logger.error(errorMessage);
            throw new UnsupportedOperationException(errorMessage);
        }
    }

    private String extractContent(String json) {
        try {
            JsonNode rootNode = objectMapper.readTree(json);
            String content = rootNode.get(CONTENT_FIELD_NAME).asText();
            return content.replaceAll("\n", "");
        } catch (JsonProcessingException e) {
            throw new RuntimeException("failed to parse json", e);
        }
    }

    private static void printErrorMessage(Throwable e) {
        if (!(e instanceof WebClientResponseException ex)) {
            logger.error("Error occurred: {}", e.getMessage(), e);
            return;
        }
        if (ex.getStatusCode().value() == HttpStatus.NOT_FOUND.value()) {
            logger.info("ignore from repo that not including pom.xml {}", ex.getMessage(), ex);
        } else if (ex.getStatusCode().value() == HttpStatus.FORBIDDEN.value() || ex.getStatusCode().value() == HttpStatus.TOO_MANY_REQUESTS.value()) {
            logger.error("Error occurred: API rate limit exceeded for user ID  {}", ex.getMessage(), ex);
            /**
             * {
             *     "message": "API rate limit exceeded for user ID 144931323. If you reach out to GitHub Support for help, please include the request ID EF65:CBAE:132BD4B7:136A4626:6565C271.",
             *     "documentation_url": "https://docs.github.com/rest/overview/rate-limits-for-the-rest-api"
             * }
             */
        } else {
            logger.error("Error occurred: {}", ex.getMessage(), ex);
        }
    }


    private enum Language {
        JAVA("Java");
        private final String language;

        Language(String language) {
            this.language = language;
        }

        public String getLanguage() {
            return language;
        }
    }

}
