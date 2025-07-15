package net.osmand.support;

import java.io.*;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.Paths;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public class MarkdownSlicer {

    public static void main(String[] args) {
        if (args.length < 2) {
            System.out.println("Usage: java MarkdownProcessor <inputDir> <outputDir>");
            return;
        }

        Path inputDir = Paths.get(args[0]);
        Path outputDir = Paths.get(args[1]);
        if (!Files.isDirectory(inputDir)) {
            System.out.println("Input directory does not exist.");
            return;
        }

        try {
            processDirectory(inputDir, outputDir);
            System.out.println("Processing complete.");
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    private static void processDirectory(Path inputDir, Path outputDir) throws IOException {
        Files.walk(inputDir)
                .filter(path -> Files.isRegularFile(path) && path.toString().endsWith(".md"))
                .forEach(file -> {
                    try {
                        processFile(file, inputDir, outputDir);
                        System.out.println("File processed: " + file);
                    } catch (IOException e) {
                        System.out.println("Error processing file: " + file);
                        e.printStackTrace();
                    }
                });
    }

    private static void processFile(Path file, Path inputDir, Path outputDir) throws IOException {
        BufferedReader reader = new BufferedReader(new FileReader(file.toFile()));
        StringBuilder currentContent = new StringBuilder();
        String currentTopicName = null;

        Path relativePath = inputDir.relativize(file.getParent());
        Pattern topicPattern = Pattern.compile("^(## |### )(?!\\[)(.+)");
        String line;
        while ((line = reader.readLine()) != null) {
            Matcher matcher = topicPattern.matcher(line);
            if (matcher.find()) {
                if (currentTopicName != null) {
                    writeToFile(outputDir, relativePath, file.toFile().getName(), currentContent.toString(), currentTopicName);
                }
                currentTopicName = matcher.group(2).trim();
                currentContent.setLength(0); // Clear content
            }
            currentContent.append(line).append(System.lineSeparator());
        }

        if (currentTopicName != null) {
            writeToFile(outputDir, relativePath, file.toFile().getName(), currentContent.toString(), currentTopicName);
        }
        reader.close();
    }

    public static String replaceTillAllReplaced(String str, String target, String replacement) {
        if (str == null || target == null || target.isEmpty() || replacement == null) {
            return str; // Return the input string as-is for invalid inputs.
        }

        while (str.contains(target)) {
            str = str.replace(target, replacement);
        }

        return str;
    }

    private static String sanitize(String name, String replacement) {
        // Replace or remove problematic characters including ':' and other special symbols
        name = name.trim();
        return name.replaceAll("[^a-zA-Z0-9_.-]", replacement)
                .replaceAll("[:*?\"<>|]", replacement)  // Additional special characters
                .replaceAll("_{2,}", replacement)
                .trim();
    }

    private static void writeToFile(Path outputDir, Path relativePath, String fileName, String answer, String topic) throws IOException {
        String subject = replaceTillAllReplaced(trim(sanitize(topic, "-").replace(".", "").toLowerCase(), '-'), "--", "-");
        String fn = fileName.substring(0, fileName.length() - 3);
        String newFileName = (relativePath.toString().isEmpty() ? "" : sanitize(relativePath.toString(), ".") + '.') + fn + '.' + sanitize(topic, "_") + ".md";
        String url = "https://osmand.net/docs/" + (relativePath.toString().isEmpty() ? "" : relativePath.toString().replace('\\', '/') + "/") + fn + "#" + subject;

        String[] lines = topic.split("\n", 1);
        String question = lines.length > 0 ? lines[0] : "";
        subject = relativePath.toString().replace('\\', ' ') + " " + subject;
        answer = "<ticket importance=\"Norm\">\n" +
                "<subject>" + subject + "</subject>\n" +
                "<question>" + trim(question, '#') + "?</question>\n" +
                "<answer>" + answer.substring(question.length() + 5) +
                "Please find more info by using URL: " + url + "</answer></ticket>";
        Path outputPath = outputDir.resolve(outputDir).resolve(newFileName);
        Files.createDirectories(outputPath.getParent());
        try (BufferedWriter writer = new BufferedWriter(new FileWriter(outputPath.toFile()))) {
            writer.write(answer);
        }
    }

    public static String trim(String str, char prefixSuffix) {
        if (str == null || str.isEmpty()) {
            return str; // Return the string as-is if it's null or empty.
        }

        int start = 0;
        int end = str.length() - 1;

        // Move the start index forward to skip the prefixSuffix characters.
        while (start <= end && str.charAt(start) == prefixSuffix) {
            start++;
        }

        // Move the end index backward to skip the prefixSuffix characters.
        while (end >= start && str.charAt(end) == prefixSuffix) {
            end--;
        }

        // Return the trimmed substring.
        return str.substring(start, end + 1);
    }
}
