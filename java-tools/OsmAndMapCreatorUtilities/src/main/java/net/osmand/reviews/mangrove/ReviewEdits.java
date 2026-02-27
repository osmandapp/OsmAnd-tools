package net.osmand.reviews.mangrove;

import com.google.common.base.Preconditions;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Sets;

import javax.validation.constraints.NotNull;
import java.util.*;
import java.util.function.Function;
import java.util.stream.Collectors;

final class ReviewEdits {
    private final static String MARESI_PREFIX = "urn:maresi:";

    /**
     * Reduces chains of edits by propagating the <code>sub</code> of the original review to the edits and removing all versions except the most recent one.
     * <p>
     * In Mangrove data, edits are represented as new reviews, but with <code>payload.action="edit"</code> and with <code>payload.sub="urn:maresi:&lt;edited review signature&gt;"</code>.
     * In order to match the edit to original subject and location information provided in the client string, this function propagates:
     * <ul>
     *     <li><code>payload.sub</code></li>
     *     <li><code>payload.metadata.client_id</code></li>
     * </ul>
     * <p>
     * from the edited review to the edit, then removes the edited review from the set. In case of multiple parallel edits of the same review, only the latest one (by <code>payload.iat</code>) is left.
     * </p>
     *
     * @param reviews a set of reviews, including all edits
     * @return a set of reviews, with only latest edits remaining, and with fields updated to match the originals as described above
     */
    public static @NotNull Set<@NotNull Review> applyEdits(Set<Review> reviews) {
        Map<String, Review> bySignature = reviews.stream().collect(Collectors.toMap(Review::signature, Function.identity()));
        Map<String, String> rootSignatures = rootSignaturesForEdits(bySignature);
        Map<String, Set<Review>> rootEdits = editsForRoots(bySignature, rootSignatures);
        Map<String, Review> rootLatestEdits = latestEditsForRoots(rootEdits);
        Set<Review> updatedEdits = updateEditsFromRoots(bySignature, rootLatestEdits);

        // output consists of:
        // - all reviews that have never been edited (are not roots) and that are themselves not edits
        // - latest edits, updated with info from their roots
        Set<String> signaturesToExclude = Sets.union(rootEdits.keySet(), rootSignatures.keySet());

        ImmutableSet.Builder<Review> result = ImmutableSet.builder();
        for (Map.Entry<String, Review> entry : bySignature.entrySet()) {
            if (!signaturesToExclude.contains(entry.getKey())) {
                result.add(entry.getValue());
            }
        }
        result.addAll(updatedEdits);
        return result.build();
    }

    private static @NotNull Set<Review> updateEditsFromRoots(Map<String, Review> reviewsBySignature, Map<String, Review> rootLatestEdits) {
        ImmutableSet.Builder<Review> updatedEdits = ImmutableSet.builder();
        for (Map.Entry<String, Review> entry : rootLatestEdits.entrySet()) {
            Review root = reviewsBySignature.get(entry.getKey());
            Review edit = entry.getValue();
            Review updatedEdit = updateEditFromRoot(edit, root);
            updatedEdits.add(updatedEdit);
        }
        return updatedEdits.build();
    }

    private static @NotNull Map<String, Review> latestEditsForRoots(Map<String, Set<Review>> rootEdits) {
        ImmutableMap.Builder<String, Review> rootLatestEdits = ImmutableMap.builder();
        for (Map.Entry<String, Set<Review>> entry : rootEdits.entrySet()) {
            @SuppressWarnings("OptionalGetWithoutIsPresent") Review latestEdit = entry.getValue().stream().max(Comparator.comparingLong(r -> r.payload().iat())).get();
            rootLatestEdits.put(entry.getKey(), latestEdit);
        }
        return rootLatestEdits.build();
    }

    private static Review updateEditFromRoot(Review edit, Review root) {
        return edit
                .withPayload(
                        edit.payload()
                                .withSub(root.payload().sub())
                                .withMetadata(edit.payload().metadata().withClientId(root.payload().metadata().clientId()))
                );
    }

    /**
     *
     * @param reviewsBySignature a map of signature -> review
     * @param rootSignatures     a map of "edit" review signature -> the signature of the root review the edit applies to
     * @return a map of root signature -> all edits of that root; only where the edit set is not empty
     */
    private static @NotNull Map<String, Set<Review>> editsForRoots(Map<String, Review> reviewsBySignature, Map<String, String> rootSignatures) {
        Map<String, Set<Review>> rootEdits = new HashMap<>();
        for (Map.Entry<String, String> entry : rootSignatures.entrySet()) {
            String editSignature = entry.getKey();
            String rootSignature = entry.getValue();
            if (!rootEdits.containsKey(rootSignature)) {
                rootEdits.put(rootSignature, new HashSet<>());
            }
            rootEdits.get(rootSignature).add(reviewsBySignature.get(editSignature));
        }
        return ImmutableMap.copyOf(rootEdits);
    }

    /**
     *
     * @param reviewsBySignature a map of signature -> review
     * @return a map of "edit" review signature -> the signature of the root review the edit applies to
     */
    private static @NotNull Map<String, String> rootSignaturesForEdits(Map<String, Review> reviewsBySignature) {
        Map<String, String> rootSignatures = new HashMap<>();
        for (Review review : reviewsBySignature.values()) {
            if ("edit".equals(review.payload().action())) {
                String parentSignature = signatureFromSub(review.payload().sub());
                String rootSignature = rootSignatures.get(parentSignature);
                if (rootSignature == null) {
                    rootSignature = findRootSignature(parentSignature, reviewsBySignature);
                }
                rootSignatures.put(review.signature(), rootSignature);
            }
        }
        return rootSignatures;
    }

    private static String findRootSignature(String signature, Map<@NotNull String, Review> reviewsBySignature) {
        Review current = reviewsBySignature.get(signature);
        while (current.payload().action() != null) {
            String parentSignature = signatureFromSub(current.payload().sub());
            current = reviewsBySignature.get(parentSignature);
        }
        return current.signature();
    }

    private static String signatureFromSub(@NotNull String sub) {
        Preconditions.checkArgument(sub.startsWith(MARESI_PREFIX), "can't extract signature from sub '%s'", sub);
        return sub.substring(MARESI_PREFIX.length());
    }

    private ReviewEdits() {
    }
}
