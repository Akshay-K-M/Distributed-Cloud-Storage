// This file implements user-facing ocs subcommands and their output formats.
package cli

import (
	"context"
	"encoding/json"
	"flag"
	"fmt"
	"os"
	"path/filepath"
	"strings"
	"text/tabwriter"
)

type stringSlice []string

func (s *stringSlice) String() string {
	return strings.Join(*s, ",")
}

func (s *stringSlice) Set(val string) error {
	*s = append(*s, val)
	return nil
}

func (c *CLI) handleCP(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("cp", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)

	var overwrite bool
	var metaFlags stringSlice
	var contentType string

	fs.BoolVar(&overwrite, "overwrite", false, "overwrite existing committed object")
	fs.Var(&metaFlags, "meta", "custom metadata key=val (can be specified multiple times)")
	fs.StringVar(&contentType, "content-type", "", "explicit content type")
	c.bindGlobalFlags(fs)

	if err := fs.Parse(reorderArgs(args)); err != nil {
		return 2
	}

	posArgs := fs.Args()
	if len(posArgs) != 2 {
		c.errorf("INVALID_USAGE", "usage: ocs cp <local> obj:<name> [flags] OR ocs cp obj:<name> <local|->")
		return 2
	}

	src, dst := posArgs[0], posArgs[1]
	srcRemote, _, _ := ParseTarget(src)
	dstRemote, _, _ := ParseTarget(dst)

	if !srcRemote && dstRemote {
		// Upload: <local> obj:<name>
		_, isID, remoteName := ParseTarget(dst)
		if isID {
			c.errorf("INVALID_USAGE", "cannot upload to raw id target %q (use obj:<name>)", dst)
			return 2
		}

		if strings.HasSuffix(dst, "/") || remoteName == "" {
			remoteName = strings.TrimSuffix(remoteName, "/")
			if remoteName != "" {
				remoteName += "/" + filepath.Base(src)
			} else {
				remoteName = filepath.Base(src)
			}
		}

		metaMap := make(map[string]string)
		for _, m := range metaFlags {
			parts := strings.SplitN(m, "=", 2)
			if len(parts) == 2 {
				metaMap[parts[0]] = parts[1]
			}
		}

		obj, err := c.client.Upload(ctx, src, remoteName, overwrite, metaMap, contentType, c.Quiet, c.Stderr)
		if err != nil {
			c.handleError(err)
			return 1
		}

		if c.JSON {
			b, _ := json.MarshalIndent(obj, "", "  ")
			fmt.Fprintln(c.Stdout, string(b))
		} else {
			fmt.Fprintf(c.Stdout, "Uploaded %s (id: %s), size: %d bytes, sha256: %s\n",
				obj.Name, obj.ObjectID, obj.Size, obj.ChecksumSHA256)
		}
		return 0

	} else if srcRemote && !dstRemote {
		// Download: obj:<name> <local|->
		objID, resolvedName, err := c.client.ResolveObjectID(ctx, src)
		if err != nil {
			c.handleError(err)
			return 1
		}

		if dst == "-" {
			// Stream to stdout
			_, err := c.client.Download(ctx, objID, c.Stdout)
			if err != nil {
				c.handleError(err)
				return 1
			}
			return 0
		}

		destPath := dst
		// If dst is an existing directory, append base name
		fi, statErr := os.Stat(dst)
		if statErr == nil && fi.IsDir() {
			baseName := filepath.Base(resolvedName)
			if baseName == "" || baseName == "." {
				baseName = objID
			}
			destPath = filepath.Join(dst, baseName)
		}

		outFile, err := os.Create(destPath)
		if err != nil {
			c.errorf("INVALID_REQUEST", "failed to create local file %s: %v", destPath, err)
			return 1
		}
		defer outFile.Close()

		_, err = c.client.Download(ctx, objID, outFile)
		if err != nil {
			_ = os.Remove(destPath)
			c.handleError(err)
			return 1
		}

		if !c.Quiet && !c.JSON {
			fmt.Fprintf(c.Stdout, "Downloaded to %s\n", destPath)
		}
		return 0
	} else {
		c.errorf("INVALID_USAGE", "exactly one of source or destination must be a remote path (obj:...)")
		return 2
	}
}

func (c *CLI) handleCat(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("cat", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	c.bindGlobalFlags(fs)

	if err := fs.Parse(reorderArgs(args)); err != nil {
		return 2
	}

	posArgs := fs.Args()
	if len(posArgs) != 1 {
		c.errorf("INVALID_USAGE", "usage: ocs cat obj:<name>")
		return 2
	}

	objID, _, err := c.client.ResolveObjectID(ctx, posArgs[0])
	if err != nil {
		c.handleError(err)
		return 1
	}

	_, err = c.client.Download(ctx, objID, c.Stdout)
	if err != nil {
		c.handleError(err)
		return 1
	}
	return 0
}

func (c *CLI) handleLS(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("ls", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)

	var longFormat bool
	var recursive bool

	fs.BoolVar(&longFormat, "l", false, "long listing format")
	fs.BoolVar(&recursive, "R", false, "recursive listing (no delimiter)")
	c.bindGlobalFlags(fs)

	if err := fs.Parse(reorderArgs(args)); err != nil {
		return 2
	}

	prefix := ""
	posArgs := fs.Args()
	if len(posArgs) > 0 {
		_, _, prefix = ParseTarget(posArgs[0])
	}

	delimiter := "/"
	if recursive {
		delimiter = ""
	}

	objects, commonPrefixes, err := c.client.List(ctx, prefix, delimiter)
	if err != nil {
		c.handleError(err)
		return 1
	}

	if c.JSON {
		res := map[string]any{
			"objects":         objects,
			"common_prefixes": commonPrefixes,
		}
		b, _ := json.MarshalIndent(res, "", "  ")
		fmt.Fprintln(c.Stdout, string(b))
		return 0
	}

	w := tabwriter.NewWriter(c.Stdout, 0, 4, 2, ' ', 0)
	for _, cp := range commonPrefixes {
		if longFormat {
			fmt.Fprintf(w, "DIR\t-\t-\t%s\n", cp)
		} else {
			fmt.Fprintln(w, cp)
		}
	}
	for _, obj := range objects {
		if longFormat {
			fmt.Fprintf(w, "%d\t%s\t%s\t%s\n", obj.Size, obj.CreatedAt, obj.ObjectID, obj.Name)
		} else {
			fmt.Fprintln(w, obj.Name)
		}
	}
	_ = w.Flush()
	return 0
}

func (c *CLI) handleStat(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("stat", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	c.bindGlobalFlags(fs)

	if err := fs.Parse(reorderArgs(args)); err != nil {
		return 2
	}

	posArgs := fs.Args()
	if len(posArgs) != 1 {
		c.errorf("INVALID_USAGE", "usage: ocs stat obj:<name>")
		return 2
	}

	objID, _, err := c.client.ResolveObjectID(ctx, posArgs[0])
	if err != nil {
		c.handleError(err)
		return 1
	}

	detail, err := c.client.GetMetadata(ctx, objID)
	if err != nil {
		c.handleError(err)
		return 1
	}

	if c.JSON {
		b, _ := json.MarshalIndent(detail, "", "  ")
		fmt.Fprintln(c.Stdout, string(b))
		return 0
	}

	fmt.Fprintf(c.Stdout, "Object ID:       %s\n", detail.ObjectID)
	fmt.Fprintf(c.Stdout, "Name:            %s\n", detail.Name)
	fmt.Fprintf(c.Stdout, "Size:            %d bytes\n", detail.Size)
	fmt.Fprintf(c.Stdout, "Content-Type:    %s\n", detail.ContentType)
	fmt.Fprintf(c.Stdout, "SHA-256:         %s\n", detail.ChecksumSHA256)
	fmt.Fprintf(c.Stdout, "Chunks:          %d\n", detail.ChunkCount)
	fmt.Fprintf(c.Stdout, "Created:         %s\n", detail.CreatedAt)
	fmt.Fprintf(c.Stdout, "Modified:        %s\n", detail.ModifiedAt)

	if len(detail.CustomMetadata) > 0 {
		var metaPairs []string
		for k, v := range detail.CustomMetadata {
			metaPairs = append(metaPairs, fmt.Sprintf("%s=%s", k, v))
		}
		fmt.Fprintf(c.Stdout, "Custom Metadata: %s\n", strings.Join(metaPairs, ", "))
	} else {
		fmt.Fprintln(c.Stdout, "Custom Metadata: (none)")
	}

	if len(detail.Chunks) > 0 {
		fmt.Fprintln(c.Stdout, "\nChunks:")
		w := tabwriter.NewWriter(c.Stdout, 0, 4, 2, ' ', 0)
		fmt.Fprintln(w, "INDEX\tCHUNK ID\tSIZE\tSHA-256\tREPLICAS")
		for _, ch := range detail.Chunks {
			var repStrs []string
			for _, r := range ch.Replicas {
				repStrs = append(repStrs, fmt.Sprintf("%s (%s)", r.WorkerID, r.State))
			}
			shaDisplay := ch.SHA256
			if len(shaDisplay) > 16 {
				shaDisplay = shaDisplay[:16] + "..."
			}
			fmt.Fprintf(w, "%d\t%s\t%d\t%s\t%s\n", ch.Index, ch.ChunkID, ch.Size, shaDisplay, strings.Join(repStrs, ", "))
		}
		_ = w.Flush()
	}
	return 0
}

func (c *CLI) handleMeta(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("meta", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	c.bindGlobalFlags(fs)

	if err := fs.Parse(reorderArgs(args)); err != nil {
		return 2
	}

	posArgs := fs.Args()
	if len(posArgs) < 2 {
		c.errorf("INVALID_USAGE", "usage: ocs meta get|set|rm obj:<name> [args]")
		return 2
	}

	action := posArgs[0]
	target := posArgs[1]
	objID, _, err := c.client.ResolveObjectID(ctx, target)
	if err != nil {
		c.handleError(err)
		return 1
	}

	switch action {
	case "get":
		detail, err := c.client.GetMetadata(ctx, objID)
		if err != nil {
			c.handleError(err)
			return 1
		}
		if c.JSON {
			b, _ := json.MarshalIndent(detail.CustomMetadata, "", "  ")
			fmt.Fprintln(c.Stdout, string(b))
		} else {
			if len(detail.CustomMetadata) == 0 {
				fmt.Fprintln(c.Stdout, "(empty)")
			} else {
				for k, v := range detail.CustomMetadata {
					fmt.Fprintf(c.Stdout, "%s=%s\n", k, v)
				}
			}
		}
		return 0

	case "set":
		if len(posArgs) < 3 {
			c.errorf("INVALID_USAGE", "usage: ocs meta set obj:<name> k1=v1 [k2=v2...]")
			return 2
		}
		patch := make(map[string]*string)
		for _, pair := range posArgs[2:] {
			parts := strings.SplitN(pair, "=", 2)
			if len(parts) != 2 {
				c.errorf("INVALID_USAGE", "invalid metadata pair %q (must be k=v)", pair)
				return 2
			}
			val := parts[1]
			patch[parts[0]] = &val
		}

		updated, err := c.client.PatchMetadata(ctx, objID, patch)
		if err != nil {
			c.handleError(err)
			return 1
		}

		if c.JSON {
			b, _ := json.MarshalIndent(updated, "", "  ")
			fmt.Fprintln(c.Stdout, string(b))
		} else {
			fmt.Fprintln(c.Stdout, "Metadata updated successfully:")
			for k, v := range updated.CustomMetadata {
				fmt.Fprintf(c.Stdout, "%s=%s\n", k, v)
			}
		}
		return 0

	case "rm":
		if len(posArgs) < 3 {
			c.errorf("INVALID_USAGE", "usage: ocs meta rm obj:<name> key1 [key2...]")
			return 2
		}
		patch := make(map[string]*string)
		for _, key := range posArgs[2:] {
			patch[key] = nil // RFC 7386 null deletes key
		}

		updated, err := c.client.PatchMetadata(ctx, objID, patch)
		if err != nil {
			c.handleError(err)
			return 1
		}

		if c.JSON {
			b, _ := json.MarshalIndent(updated, "", "  ")
			fmt.Fprintln(c.Stdout, string(b))
		} else {
			fmt.Fprintln(c.Stdout, "Metadata updated successfully:")
			for k, v := range updated.CustomMetadata {
				fmt.Fprintf(c.Stdout, "%s=%s\n", k, v)
			}
		}
		return 0

	default:
		c.errorf("INVALID_USAGE", "unknown meta action %q (must be get, set, or rm)", action)
		return 2
	}
}

func (c *CLI) handleRM(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("rm", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)

	var recursive bool
	fs.BoolVar(&recursive, "r", false, "recursive delete")
	c.bindGlobalFlags(fs)

	if err := fs.Parse(reorderArgs(args)); err != nil {
		return 2
	}

	posArgs := fs.Args()
	if len(posArgs) != 1 {
		c.errorf("INVALID_USAGE", "usage: ocs rm obj:<name> [-r]")
		return 2
	}

	target := posArgs[0]
	if recursive {
		_, _, prefix := ParseTarget(target)
		objects, _, err := c.client.List(ctx, prefix, "")
		if err != nil {
			c.handleError(err)
			return 1
		}
		for _, obj := range objects {
			if err := c.client.Delete(ctx, obj.ObjectID); err != nil {
				c.handleError(err)
				return 1
			}
			if !c.Quiet {
				fmt.Fprintf(c.Stdout, "Deleted %s (%s)\n", obj.Name, obj.ObjectID)
			}
		}
		return 0
	}

	objID, resolvedName, err := c.client.ResolveObjectID(ctx, target)
	if err != nil {
		c.handleError(err)
		return 1
	}

	if err := c.client.Delete(ctx, objID); err != nil {
		c.handleError(err)
		return 1
	}

	if !c.Quiet {
		fmt.Fprintf(c.Stdout, "Deleted %s (%s)\n", resolvedName, objID)
	}
	return 0
}

func (c *CLI) handleStatus(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("status", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	c.bindGlobalFlags(fs)

	if err := fs.Parse(reorderArgs(args)); err != nil {
		return 2
	}

	status, err := c.client.AdminStatus(ctx)
	if err != nil {
		c.handleError(err)
		return 1
	}

	if c.JSON {
		b, _ := json.MarshalIndent(status, "", "  ")
		fmt.Fprintln(c.Stdout, string(b))
		return 0
	}

	fmt.Fprintln(c.Stdout, "Config:")
	fmt.Fprintf(c.Stdout, "  Replication Factor: %d\n", status.Config.ReplicationFactor)
	fmt.Fprintf(c.Stdout, "  Write Acks:         %d\n", status.Config.WriteAcks)
	fmt.Fprintf(c.Stdout, "  Chunk Size:         %d bytes\n\n", status.Config.ChunkSize)

	fmt.Fprintln(c.Stdout, "Workers:")
	w := tabwriter.NewWriter(c.Stdout, 0, 4, 2, ' ', 0)
	fmt.Fprintln(w, "  WORKER ID\tSTATUS\tADDRESS\tUSED BYTES\tCHUNKS\tLAST SEEN")
	for _, wk := range status.Workers {
		fmt.Fprintf(w, "  %s\t%s\t%s\t%s\t%d\t%dms ago\n",
			wk.WorkerID, wk.Status, wk.Address, formatBytes(wk.UsedBytes), wk.ChunkCount, wk.LastSeenMSAgo)
	}
	_ = w.Flush()

	fmt.Fprintln(c.Stdout, "\nObjects:")
	fmt.Fprintf(c.Stdout, "  Committed: %d\n", status.Objects.Committed)
	fmt.Fprintf(c.Stdout, "  Uploading: %d\n", status.Objects.Uploading)
	fmt.Fprintf(c.Stdout, "  Deleting:  %d\n\n", status.Objects.Deleting)

	fmt.Fprintln(c.Stdout, "Chunks:")
	fmt.Fprintf(c.Stdout, "  Total:             %d\n", status.Chunks.Total)
	fmt.Fprintf(c.Stdout, "  Under-replicated:  %d\n", status.Chunks.UnderReplicated)
	fmt.Fprintf(c.Stdout, "  Over-replicated:   %d\n", status.Chunks.OverReplicated)
	fmt.Fprintf(c.Stdout, "  Corrupt Replicas:  %d\n", status.Chunks.CorruptReplicas)
	fmt.Fprintf(c.Stdout, "  Lost:              %d\n", status.Chunks.Lost)

	return 0
}

func (c *CLI) handleVerify(ctx context.Context, args []string) int {
	fs := flag.NewFlagSet("verify", flag.ContinueOnError)
	fs.SetOutput(c.Stderr)
	c.bindGlobalFlags(fs)

	if err := fs.Parse(reorderArgs(args)); err != nil {
		return 2
	}

	posArgs := fs.Args()
	if len(posArgs) != 2 {
		c.errorf("INVALID_USAGE", "usage: ocs verify obj:<name> <local-file>")
		return 2
	}

	var remoteTarget, localFile string
	isRemote0, _, _ := ParseTarget(posArgs[0])
	isRemote1, _, _ := ParseTarget(posArgs[1])

	if isRemote0 && !isRemote1 {
		remoteTarget, localFile = posArgs[0], posArgs[1]
	} else if !isRemote0 && isRemote1 {
		remoteTarget, localFile = posArgs[1], posArgs[0]
	} else {
		c.errorf("INVALID_USAGE", "usage: ocs verify obj:<name> <local-file>")
		return 2
	}

	// Stream local file and compute SHA256
	f, err := os.Open(localFile)
	if err != nil {
		c.errorf("INVALID_REQUEST", "failed to open local file %s: %v", localFile, err)
		return 1
	}
	defer f.Close()

	localSHA, err := SHA256Reader(f)
	if err != nil {
		c.errorf("INTERNAL", "failed to hash local file: %v", err)
		return 1
	}

	// Resolve remote object and stream to compute SHA256
	objID, _, err := c.client.ResolveObjectID(ctx, remoteTarget)
	if err != nil {
		c.handleError(err)
		return 1
	}

	resp, err := c.client.Download(ctx, objID, nil)
	if err != nil {
		c.handleError(err)
		return 1
	}
	defer resp.Body.Close()

	remoteSHA, err := SHA256Reader(resp.Body)
	if err != nil {
		c.errorf("INTERNAL", "failed to hash remote object: %v", err)
		return 1
	}

	if HexEqual(localSHA, remoteSHA) {
		fmt.Fprintln(c.Stdout, "OK")
		return 0
	} else {
		fmt.Fprintln(c.Stdout, "MISMATCH")
		return 1
	}
}
